using Npgsql;
using log4net;

namespace PostgresDbSyncApp.Utils
{
    public static class DbHelper
    {
        private static readonly ILog _logger = LogManager.GetLogger(typeof(DbHelper));

        public static async Task<string?> GenerateTableDDLAsync(NpgsqlConnection connection, string tableName)
        {
            var query = $@"
                SELECT 'CREATE TABLE IF NOT EXISTS ' || quote_ident(table_schema) || '.' || quote_ident(table_name) ||
                E'\n(\n' || string_agg(
                    '    ' || quote_ident(column_name) || ' ' || data_type
                    || COALESCE('(' || character_maximum_length || ')', '') 
                    || CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                    E',\n'
                ) || E'\n);\n'
                FROM information_schema.columns
                WHERE table_name = @table
                GROUP BY table_schema, table_name;";

            await using var cmd = new NpgsqlCommand(query, connection);
            cmd.Parameters.AddWithValue("@table", tableName);
            var ddl = await cmd.ExecuteScalarAsync();
            return ddl?.ToString();
        }

        public static async Task ExecuteDDLAsync(NpgsqlConnection connection, string ddl)
        {
            await using var cmd = new NpgsqlCommand(ddl, connection);
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task CopyDataAsync(NpgsqlConnection source, NpgsqlConnection target, string table)
        {
            var copyFrom = $"COPY {table} TO STDOUT (FORMAT BINARY)";
            var copyTo = $"COPY {table} FROM STDIN (FORMAT BINARY)";

            await using var reader = await source.BeginRawBinaryCopyAsync(copyFrom);
            await using var writer = await target.BeginRawBinaryCopyAsync(copyTo);

            byte[] buffer = new byte[65536];
            int totalBytes = 0;
            int bytesRead;

            while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                await writer.WriteAsync(buffer.AsMemory(0, bytesRead));
                totalBytes += bytesRead;
                if (totalBytes >= 50 * 1024 * 1024)
                {
                    _logger.Info($"Copied {totalBytes / (1024 * 1024)} MB for table '{table}'...");
                    totalBytes = 0;
                }
            }

            await writer.CompleteAsync();
            _logger.Info($"Binary COPY complete for table '{table}'.");
        }
    }
}
