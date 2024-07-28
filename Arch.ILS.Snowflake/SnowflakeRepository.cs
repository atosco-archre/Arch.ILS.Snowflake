
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Reflection;

using Studio.Core;

namespace Arch.ILS.Snowflake
{
    public class SnowflakeRepository : IRepository
    {
        private const string MessageAttemptFailedFormat = "Error occurred whilst attempting to execute '{0}'";

        //private const int DefaultMinimumPoolSize = 1;
        //private const int DefaultMaximumPoolSize = 256;     /* 256 Concurrent connections */

        //private const int DefaultTimeout = 10;               /* 10 seconds */

        public SnowflakeRepository(string connectionString)
        {
            ConnectionString = connectionString;
        }

        private string ConnectionString { get; }

        public void ExecuteSql(string commandText)
        {
            using SnowflakeDbConnection connection = new(ConnectionString);
            
            try
            {
                connection.Open();
                SnowflakeDbCommand command = new SnowflakeDbCommand(connection, commandText)
                {
                    CommandType = CommandType.Text
                };
                command.ExecuteNonQuery();
                connection.Close();
            }
            catch (Exception exception)
            {
                throw new RepositoryException(string.Format(MessageAttemptFailedFormat, commandText), exception);
            }
        }

        public void Execute(string command, params object[] parameterValues)
        {
            Execute(SetParameters(new SnowflakeDbCommand(new(ConnectionString), command), parameterValues));
        }

        public void Execute(DbCommand command)
        {
            using SnowflakeDbConnection connection = new (ConnectionString);
            
            try
            {
                connection.Open();
                command.Connection = connection;
                command.ExecuteNonQuery();
                connection.Close();
            }
            catch (Exception exception)
            {
                throw new RepositoryException(string.Format(MessageAttemptFailedFormat, command.CommandText), exception);
            }
        }

        public IDataReader ExecuteReaderSql(string commandText)
        {
            try
            {
                SnowflakeDbConnection connection = new(ConnectionString);
                connection.Open();

                SnowflakeDbCommand command = new SnowflakeDbCommand(connection, commandText)
                    {
                        CommandType = CommandType.Text
                    };

                return command.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception exception)
            {
                throw new RepositoryException(string.Format(MessageAttemptFailedFormat, commandText), exception);
            }
        }

        public IDataReader ExecuteReader(string command, params object[] parameterValues)
        {
            return ExecuteReader(SetParameters(new SnowflakeDbCommand(new(ConnectionString), command), parameterValues));
        }

        public IDataReader ExecuteReader(string command)
        {
            return ExecuteReader(new SnowflakeDbCommand(new(ConnectionString), command));
        }

        public IDataReader ExecuteReader(DbCommand command)
        {
            try
            {
                SnowflakeDbConnection connection = new (ConnectionString);
                connection.Open();
                command.Connection = connection;
                return command.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception exception)
            {
                throw new RepositoryException(string.Format(MessageAttemptFailedFormat, command.CommandText), exception);
            }
        }

        protected static DbCommand SetParameters(DbCommand command, IList<object> parameterValues)
        {
            if (command.Parameters.Count - 1 != parameterValues.Count)
                throw new ArgumentException($"Expected '{command.Parameters.Count - 1}' parameter values but found '{parameterValues.Count}'", nameof(parameterValues));

            for (int i = 0; i < parameterValues.Count; i++)
                command.Parameters[i + 1].Value = parameterValues[i];

            return command;
        }

        public void WriteToTable(IDataReader dataReader, string tableName, int batchSize = 10000, int timeout = 300)
        {
            throw new NotImplementedException();
        }
    }
}
