using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using log4net;
using log4net.Config;
using System.Reflection;
using PostgresDbSyncApp.Interfaces;
using PostgresDbSyncApp.Services;
using PostgresDbSyncApp.Models;
using System.IO;

namespace PostgresDbSyncApp
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);

            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
            var logger = LogManager.GetLogger(typeof(Program));

            try
            {
                builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                builder.Services.Configure<AppSettings>(builder.Configuration);
                builder.Services.AddSingleton<ISchemaSyncService, SchemaSyncService>();

                var app = builder.Build();
                var syncService = app.Services.GetRequiredService<ISchemaSyncService>();
                await syncService.SyncSchemasAsync();

                logger.Info("Schema synchronization completed.");
            }
            catch (Exception ex)
            {
                logger.Error("An error occurred during synchronization.", ex);
            }
        }
    }
}
