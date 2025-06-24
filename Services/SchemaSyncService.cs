using Microsoft.Extensions.Options;
using Npgsql;
using PostgresDbSyncApp.Interfaces;
using PostgresDbSyncApp.Models;
using PostgresDbSyncApp.Utils;
using log4net;
using Polly;

namespace PostgresDbSyncApp.Services
{
    public class SchemaSyncService : ISchemaSyncService
    {
        private readonly AppSettings _settings;
        private static readonly ILog _logger = LogManager.GetLogger(typeof(SchemaSyncService));

        public SchemaSyncService(IOptions<AppSettings> settings)
        {
            _settings = settings.Value;
        }

        public async Task SyncSchemasAsync()
        {
            var tables = _settings.SyncOptions.TablesToSync;
            var maxParallel = Math.Min(4, tables.Count);

            await Parallel.ForEachAsync(tables, new ParallelOptions { MaxDegreeOfParallelism = maxParallel }, async (table, token) =>
            {
                var policy = Policy
                    .Handle<Exception>()
                    .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(2 * i), (ex, ts) =>
                    {
                        _logger.Warn($"Retry {table} due to: {ex.Message}");
                    });

                await policy.ExecuteAsync(async () =>
                {
                    using var sourceConn = new NpgsqlConnection(_settings.SourceDb.ConnectionString);
                    using var targetConn = new NpgsqlConnection(_settings.TargetDb.ConnectionString);
                    await sourceConn.OpenAsync();
                    await targetConn.OpenAsync();

                    _logger.Info($"[START] {table}");

                    var ddl = await DbHelper.GenerateTableDDLAsync(sourceConn, table);
                    if (!string.IsNullOrWhiteSpace(ddl))
                        await DbHelper.ExecuteDDLAsync(targetConn, ddl);

                    if (_settings.SyncOptions.SyncData)
                        await DbHelper.CopyDataAsync(sourceConn, targetConn, table);

                    _logger.Info($"[DONE] {table}");
                });
            });
        }
    }
}
