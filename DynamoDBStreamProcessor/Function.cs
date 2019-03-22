using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using System.Data.SqlClient;

using System.Data;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace DynamoDBStreamProcessor
{
    public class Function
    {

        //These are all of the database attributes
        private static string dbname = "UnicornWarehouse";

        private static string rdsDNSName = "unicornwarehouse.ceobfnxueorx.us-east-1.rds.amazonaws.com";

        private static string rdsUserName = "developer";

        private static string rdsPassword = "ILove.Net!";
        public void FunctionHandler(DynamoDBEvent dynamoDbEvent, ILambdaContext context)
        {
            Console.WriteLine($".NET Log - Beginning to process {dynamoDbEvent.Records.Count} records...");
            //The list of records to write to SQL
            List<Ride> recordsToPersist = new List<Ride>();
            //Build the connection string
            string connectionString = BuildConnectionString();
            //for each of the records in the stream, process them one by one
            foreach (var record in dynamoDbEvent.Records)
            {
                Console.WriteLine($".NET Log - Event ID: {record.EventID}");
                Console.WriteLine($".NET Log - Event Name: {record.EventName}");
                //This is assigning the temporary variable the 'new' row data that was saved to dynamodb
                var attributeMap = record.Dynamodb.NewImage;
                if (attributeMap.Count > 0) // If item does not exist, attributeMap.Count will be 0
                {
                    Ride r = new Ride();
                    //Get the value by the name 'RequestTime'  the .S means grab a string
                    r.RequestTime = DateTime.Parse(attributeMap["RequestTime"].S);
                    r.RideId = attributeMap["RideId"].S;
                    r.UserName = attributeMap["User"].S;
                    r.UnicornName = attributeMap["UnicornName"].S;
                    r.UserName = attributeMap["User"].S;
                    //The unicorn value is a 'MAP' meaning it is an json serialization of an object stored in a column
                    var unicorn = attributeMap["Unicorn"].M;
                    //Now that you have the Map, grab the attributes from the unicorn
                    r.Gender = unicorn["Gender"].S;
                    r.Color = unicorn["Color"].S;

                        
                    //add to the list of the records to write to SQL Server
                    recordsToPersist.Add(r);
                }
            }
            //Now write each record to SQL Server
            foreach (var ride in recordsToPersist)
            {
                PersistRecord(ride, connectionString);
            }
            

            Console.WriteLine(".NET Log - Stream processing complete.");
        }

        //Code to write to SQL Server
        private void PersistRecord(Ride ride, string connectionString)
        {
            try
            {
                //Make your connection
                using (var connection = new SqlConnection(connectionString))

                {
                   
                    connection.Open();
                    //Insert statement for SQL
                    string hackInsert = "INSERT INTO dbo.Rides (RideId, RequestTime, UnicornName, UserName, Color, Gender) VALUES (@RideId, @RequestTime, @UnicornName, @UserName, @Color, @Gender);";


                    //open the command, populate the parameters, the run the query (insert)
                    using (SqlCommand command = new SqlCommand(hackInsert, connection))
                    {

                        command.Parameters.Add("@RideId", SqlDbType.VarChar);
                        command.Parameters["@RideId"].Value = ride?.RideId;

                        command.Parameters.Add("@RequestTime", SqlDbType.DateTime);
                        command.Parameters["@RequestTime"].Value = ride?.RequestTime;

                        command.Parameters.Add("@UnicornName", SqlDbType.VarChar);
                        command.Parameters["@UnicornName"].Value = ride?.UnicornName;

                        command.Parameters.Add("@UserName", SqlDbType.VarChar);
                        command.Parameters["@UserName"].Value = ride?.UserName;

                        command.Parameters.Add("@Color", SqlDbType.VarChar);
                        command.Parameters["@Color"].Value = ride?.Color;

                        command.Parameters.Add("@Gender", SqlDbType.VarChar);
                        command.Parameters["@Gender"].Value = ride?.Gender;
                        //do the insert
                        command.ExecuteNonQuery();
                        
                      
                    }
                }



            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception " + ex.Message);
            }
           
        }

        //Code to build out a connection string
        private string BuildConnectionString()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

            builder.DataSource = rdsDNSName;
            builder.UserID = rdsUserName;
            builder.Password = rdsPassword;
            builder.InitialCatalog = dbname;
            return builder.ConnectionString;

        }
    }
}
