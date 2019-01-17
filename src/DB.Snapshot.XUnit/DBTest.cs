namespace DB.Snapshot.XUnit
{
    using System;

    public class DBTest : IDisposable
    {
        private readonly DatabaseSnapshot _databaseSnapshot;

        protected DBTest(string dbName, string snapshotPath, string snapshotName, string connectionString)
        {
            _databaseSnapshot = new DatabaseSnapshot(dbName, snapshotPath, snapshotName, connectionString);

            try
            {
                _databaseSnapshot.DeleteSnapshot();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message, e);
            }

            _databaseSnapshot.CreateSnapshot();
        }

        ~DBTest()
        {
            ReleaseUnmanagedResources();
        }

        private void ReleaseUnmanagedResources()
        {
            _databaseSnapshot.RestoreSnapshot();
            _databaseSnapshot.DeleteSnapshot();
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }
    }
}