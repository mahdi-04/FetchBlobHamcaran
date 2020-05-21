using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.OracleClient;
using System.Data.SqlClient;

namespace FetchBlobHamcaran
{
    public class DataAccess
    {
        #region~( Fields )~

        public static IDbConnection Connection
        {
            get;
            set;
        }

        public enum ConnectionStringType
        {
            Eorg,
            Other
        }

        public static DatabaseType DbConnectType
        {
            get
            {
                var dbType = GetAppSettings("DbType");
                var databaseType = new DatabaseType();
                switch (dbType)
                {
                    case "Oracle":
                        databaseType = DatabaseType.Oracle;
                        break;
                    case "SqlServer":
                        databaseType = DatabaseType.SqlServer;
                        break;
                    case "ODBC":
                        databaseType = DatabaseType.Odbc;
                        break;
                    case "OleDB":
                        databaseType = DatabaseType.OleDB;
                        break;
                }
                return databaseType;
            }
        }

        public enum DatabaseType
        {
            SqlServer,
            Oracle,
            Odbc,
            OleDB
        }

        #endregion

        #region~( Data Access )~

        public static IDbConnection CreateConnection(string connectionString)
        {
            IDbConnection dbConnection = null;
            switch (DbConnectType)
            {
                case DatabaseType.SqlServer:
                    dbConnection = new SqlConnection(connectionString);
                    break;
                case DatabaseType.Oracle:
                    dbConnection = new OracleConnection(connectionString);
                    break;
                case DatabaseType.Odbc:
                    dbConnection = new OdbcConnection(connectionString);
                    break;
                case DatabaseType.OleDB:
                    dbConnection = new OleDbConnection(connectionString);
                    break;
            }
            return dbConnection;
        }

        public static IDbCommand CreateCommand(string sqlStatment)
        {
            lock (Connection)
            {
                var command = Connection.CreateCommand();
                command.CommandText = sqlStatment;
                return command;
            }
        }

        public static IDbDataAdapter CreateDataAdapter(string sqlStatment)
        {
            var command = CreateCommand(sqlStatment);
            IDbDataAdapter dataAdapter = null;
            switch (DbConnectType)
            {
                case DatabaseType.SqlServer:
                    dataAdapter = new SqlDataAdapter((SqlCommand)command);
                    break;
                case DatabaseType.Oracle:
                    dataAdapter = new OracleDataAdapter((OracleCommand)command);
                    break;
                case DatabaseType.Odbc:
                    dataAdapter = new OdbcDataAdapter((OdbcCommand)command);
                    break;
                case DatabaseType.OleDB:
                    dataAdapter = new OleDbDataAdapter((OleDbCommand)command);
                    break;
            }
            return dataAdapter;
        }

        public static IDataReader ExecuteReader(string sqlStatment)
        {
            var dataTable = ExecuteDataTable(sqlStatment);
            var dataReader = dataTable.CreateDataReader();
            return dataReader;
        }

        public static DataTable ExecuteDataTable(string sqlStatment)
        {
            var ds = ExecuteDataset(sqlStatment);
            if (ds != null)
                if (ds.Tables.Count != 0)
                {
                    var dataTable = ds.Tables[0];
                    return dataTable;
                }
            return null;
        }

        public static DataSet ExecuteDataset(string sqlStatment)
        {
            var dataSet = new DataSet();
            var dataAdapter = CreateDataAdapter(sqlStatment);
            lock (dataAdapter)
            {
                lock (Connection)
                {
                    try
                    {
                        ConnectionOpen();
                        dataAdapter.Fill(dataSet);
                    }
                    finally
                    {
                        ConnectionClose();
                    }
                    return dataSet;
                }
            }
        }

        public static IDbCommand NewCommand(string commandText, IDbConnection connection, IDbTransaction transaction)
        {
            IDbCommand ret = null;
            switch (DbConnectType)
            {
                case DatabaseType.Oracle:
                    ret = new OracleCommand(commandText, (OracleConnection)connection, (OracleTransaction)transaction);
                    break;
                case DatabaseType.SqlServer:
                    ret = new SqlCommand(commandText, (SqlConnection)connection, (SqlTransaction)transaction);
                    break;
                case DatabaseType.OleDB:
                    ret = new OleDbCommand(commandText, (OleDbConnection)connection, (OleDbTransaction)transaction);
                    break;
                case DatabaseType.Odbc:
                    ret = new OdbcCommand(commandText, (OdbcConnection)connection, (OdbcTransaction)transaction);
                    break;

            }
            return ret;
        }

        public static void DeriveParameters(IDbCommand cmd)
        {
            try
            {
                switch (DbConnectType)
                {
                    case DatabaseType.Oracle:
                        OracleCommandBuilder.DeriveParameters((OracleCommand)cmd);
                        break;
                    case DatabaseType.SqlServer:
                        SqlCommandBuilder.DeriveParameters((SqlCommand)cmd);
                        break;
                }

            }
            catch (Exception ex)
            {
                throw new Exception("Invalid parameters in Procedure call!", ex);
            }
        }

        private static void AttachParameters(IDbCommand command, IEnumerable<IDbDataParameter> commandParameters)
        {
            foreach (var p in commandParameters)
            {
                if ((p.Direction == ParameterDirection.InputOutput) && (p.Value == null))
                {
                    p.Value = DBNull.Value;
                }
                command.Parameters.Add(p);
            }
        }

        public static void AssignParameterValues(IDbDataParameter[] commandParameters, object[] parameterValues)
        {
            if ((commandParameters == null) || (parameterValues == null))
            {
                return;
            }
            try
            {
                var ivalue = 0;
                var j = commandParameters.Length;
                for (var i = 0; i < j; i++)
                {
                    var param = commandParameters[i];
                    if (DbConnectType == DatabaseType.SqlServer)
                    {
                        if (param.Direction == ParameterDirection.Input && param.SourceColumn == "")
                        {
                            param.Value = parameterValues[ivalue++];
                        }
                    }
                    else
                    {
                        if ((param.Direction == ParameterDirection.Input || param.Direction == ParameterDirection.InputOutput) && param.SourceColumn == "")
                        {
                            param.Value = parameterValues[ivalue++];
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Parameters are not matched!", ex);
            }
        }

        public static void PrepareCommand(IDbCommand command, IDbConnection connection, IDbTransaction transaction, CommandType commandType, string commandText, IDbDataParameter[] commandParameters)
        {
            if (connection.State != ConnectionState.Open)
            {
                try
                {
                    connection.Open();
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch (Exception)
                {
                }
            }
            command.Connection = connection;
            command.CommandText = commandText;
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            command.CommandType = commandType;
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }
        }

        public static void ExecuteDataset(DataSet result, IDbConnection connection, IDbTransaction transaction, CommandType commandType, string commandText, params IDbDataParameter[] commandParameters)
        {
            var da = CreateDataAdapter(string.Empty);
            var cmd = da.SelectCommand;
            PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);
            try
            {
                ((DbDataAdapter)da).Fill(result);
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch (Exception)
            {
            }
            cmd.Parameters.Clear();
        }

        private static void ConnectionOpen()
        {
            if (Connection.State == ConnectionState.Closed)
                Connection.Open();
        }

        private static void ConnectionClose()
        {
            Connection.Close();
            ConnectToDB();
        }

        public static void ConnectToDB()
        {
            ConnectToDB(ConnectionStringType.Other);
        }

        public static void ConnectToDB(ConnectionStringType connectionStringType)
        {
            var connectionString = GetConnectionString(connectionStringType == ConnectionStringType.Eorg ? "ConnectionString" : "SourceConnectionString");

            Connection = CreateConnection(connectionString);
        }

        private static string GetConnectionString(string name)
        {
            return GetAppSettings(name);
        }

        public static object ExecuteScaler(string sqlStatment)
        {
            var command = CreateCommand(sqlStatment);
            lock (Connection)
            {
                object scalar;
                try
                {
                    ConnectionOpen();
                    scalar = command.ExecuteScalar();
                }
                finally
                {
                    ConnectionClose();
                }
                return scalar;
            }
        }

        public static void ExecuteNoneQuery(string sqlStatment)
        {
            var command = CreateCommand(sqlStatment);
            lock (Connection)
            {
                try
                {
                    ConnectionOpen();
                    command.ExecuteNonQuery();
                }
                finally
                {
                    ConnectionClose();
                }
            }
        }

        public static string GetAppSettings(string appSettingsKeyName)
        {
            var keyValue = ConfigurationManager.AppSettings[appSettingsKeyName];
            return keyValue;
        }

        public static void UpdateEorgIDField(string tableName, string eorgID, decimal value, string idFieldName, string id)
        {
            var sqlStatment = String.Format("UPDATE {0} SET {1} = '{2}' WHERE {3} = '{4}'", tableName, eorgID, value, idFieldName, id);
            ExecuteNoneQuery(sqlStatment);
        }

        #endregion
    }
}
