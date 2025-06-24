namespace PostgresDbSyncApp.Interfaces
{
    public interface ISchemaSyncService
    {
        Task SyncSchemasAsync();
    }
}
