namespace PostgresDbSyncApp.Models
{
    public class AppSettings
    {
        public DbConfig SourceDb { get; set; }
        public DbConfig TargetDb { get; set; }
        public SyncOptions SyncOptions { get; set; }
    }

    public class DbConfig
    {
        public string ConnectionString { get; set; }
    }

    public class SyncOptions
    {
        public List<string> SchemasToSync { get; set; }
        public List<string> TablesToSync { get; set; }
        public bool SyncData { get; set; }
    }
}
