using System;
using System.Data.SqlClient;

class Hnp_DatabaseSynchronizer
{
    static void Main()
    {
        Console.WriteLine("Enter source database connection details:");
        string sourceConnectionString = GetConnectionString();

        Console.WriteLine("\nEnter target database connection details:");
        string targetConnectionString = GetConnectionString();

        using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
        using (SqlConnection targetConnection = new SqlConnection(targetConnectionString))
        {
            try
            {
                sourceConnection.Open();
                targetConnection.Open();

                string sourceTable = "SourceTable";
                string targetTable = "TargetTable";

                // Synchronize from source to target
                Synchronize(sourceConnection, targetConnection, sourceTable, targetTable);

                // Synchronize from target to source
                Synchronize(targetConnection, sourceConnection, targetTable, sourceTable);

                Console.WriteLine("Databases synchronized successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred during database synchronization: " + ex.Message);
            }
        }

        Console.ReadLine();
    }

    static void Synchronize(SqlConnection sourceConnection, SqlConnection targetConnection, string sourceTable, string targetTable)
    {
        string query = $"SELECT * FROM {sourceTable}";
        using (SqlCommand command = new SqlCommand(query, sourceConnection))
        using (SqlDataReader reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                string id = reader.GetString(0);
                string column1 = reader.GetString(1);
                int column2 = reader.GetInt32(2);
                DateTime column3 = reader.GetDateTime(3);

                string existenceCheckQuery = $"SELECT COUNT(*) FROM {targetTable} WHERE ID = @ID";
                using (SqlCommand existenceCheckCommand = new SqlCommand(existenceCheckQuery, targetConnection))
                {
                    existenceCheckCommand.Parameters.AddWithValue("@ID", id);
                    int existenceResult = (int)existenceCheckCommand.ExecuteScalar();

                    if (existenceResult > 0)
                    {
                        string updateQuery = $"UPDATE {targetTable} SET Column1 = @Column1, Column2 = @Column2, Column3 = @Column3 WHERE ID = @ID";
                        using (SqlCommand updateCommand = new SqlCommand(updateQuery, targetConnection))
                        {
                            updateCommand.Parameters.AddWithValue("@Column1", column1);
                            updateCommand.Parameters.AddWithValue("@Column2", column2);
                            updateCommand.Parameters.AddWithValue("@Column3", column3);
                            updateCommand.Parameters.AddWithValue("@ID", id);

                            updateCommand.ExecuteNonQuery();
                        }

                        Console.WriteLine($"Record with ID {id} updated in the target database.");
                    }
                    else
                    {
                        string insertQuery = $"INSERT INTO {targetTable} (ID, Column1, Column2, Column3) VALUES (@ID, @Column1, @Column2, @Column3)";
                        using (SqlCommand insertCommand = new SqlCommand(insertQuery, targetConnection))
                        {
                            insertCommand.Parameters.AddWithValue("@ID", id);
                            insertCommand.Parameters.AddWithValue("@Column1", column1);
                            insertCommand.Parameters.AddWithValue("@Column2", column2);
                            insertCommand.Parameters.AddWithValue("@Column3", column3);

                            insertCommand.ExecuteNonQuery();
                        }

                        Console.WriteLine($"Record with ID {id} inserted into the target database.");
                    }
                }
            }
        }
    }

    static string GetConnectionString()
    {
        Console.Write("Server: ");
        string server = Console.ReadLine();

        Console.Write("Database: ");
        string database = Console.ReadLine();

        Console.Write("Username: ");
        string username = Console.ReadLine();

        Console.Write("Password: ");
        string password = Console.ReadLine();

        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        builder.DataSource = server;
        builder.InitialCatalog = database;
        builder.UserID = username;
        builder.Password = password;

        return builder.ToString();
    }
}
