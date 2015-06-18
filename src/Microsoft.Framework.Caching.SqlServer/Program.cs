// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Framework.Logging;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class Program
    {
        private string _connectionString;
        private string _schemaName;
        private string _tableName;

        private readonly ILogger _logger;

        public Program()
        {
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            _logger = loggerFactory.CreateLogger<Program>();
        }

        public void Main(string[] args)
        {
            SwitchType switchType = SwitchType.None;
            for (var i = 0; i < args.Length; i++)
            {
                if (switchType != SwitchType.None)
                {
                    switch (switchType)
                    {
                        case SwitchType.ConnectionString:
                            _connectionString = args[i];
                            break;
                        case SwitchType.SchemaName:
                            _schemaName = args[i];
                            break;
                        case SwitchType.TableName:
                            _tableName = args[i];
                            break;
                    }

                    switchType = SwitchType.None;
                    continue;
                }

                switch (args[i].ToLower())
                {
                    case "--connectionstring":
                    case "--cs":
                        switchType = SwitchType.ConnectionString;
                        break;
                    case "--schemaname":
                    case "--sn":
                        switchType = SwitchType.SchemaName;
                        break;
                    case "--tablename":
                    case "--tn":
                        switchType = SwitchType.TableName;
                        break;
                }
            }

            if (string.IsNullOrEmpty(_connectionString)
                || string.IsNullOrEmpty(_schemaName)
                || string.IsNullOrEmpty(_tableName))
            {
                Console.WriteLine("Invalid input.");
                ShowHelp();
                return;
            }

            CreateTableAndIndexes();
        }

        private void ShowHelp()
        {
            Console.WriteLine("Example usage: dnx . create-sqlservercache --connectionString|--cs <connectionstring> " +
                "--schemaName|--sn <schemaname> --tabeName|--tn <tablename> ");
        }

        private void CreateTableAndIndexes()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var sqlQueries = new SqlQueries(_schemaName, _tableName);
                var command = new SqlCommand(sqlQueries.TableInfo, connection);
                var reader = command.ExecuteReader(CommandBehavior.SingleRow);
                if (reader.Read())
                {
                    _logger.LogWarning(
                        $"Table with schema '{_schemaName}' and name '{_tableName}' already exists. " +
                        "Provide a different table name and try again.");
                    return;
                }

                reader.Dispose();

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        command = new SqlCommand(sqlQueries.CreateTable, connection, transaction);
                        command.ExecuteNonQuery();

                        command = new SqlCommand(
                            sqlQueries.CreateNonClusteredIndexOnExpirationTime,
                            connection,
                            transaction);
                        command.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("An error occurred while trying to create the table and index.", ex);
                        transaction.Rollback();
                    }
                }
            }
        }

        private enum SwitchType
        {
            None,
            ConnectionString,
            SchemaName,
            TableName
        }
    }
}