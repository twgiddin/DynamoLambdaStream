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

        private static string dbname = "UnicornWarehouse";

        private static string rdsDNSName = "unicornwarehouse.ceobfnxueorx.us-east-1.rds.amazonaws.com";

        private static string rdsUserName = "developer";

        private static string rdsPassword = "ILove.Net!";
        public void FunctionHandler(DynamoDBEvent dynamoDbEvent, ILambdaContext context)
        {
            Console.WriteLine($".NET Log - Beginning to process {dynamoDbEvent.Records.Count} records...");
            List<Ride> recordsToPersist = new List<Ride>();
            string connectionString = BuildConnectionString();

            foreach (var record in dynamoDbEvent.Records)
            {
                Console.WriteLine($".NET Log - Event ID: {record.EventID}");
                Console.WriteLine($".NET Log - Event Name: {record.EventName}");

                var attributeMap = record.Dynamodb.NewImage;
                if (attributeMap.Count > 0) // If item does not exist, attributeMap.Count will be 0
                {
                    Ride r = new Ride();
                    r.RequestTime = DateTime.Parse(attributeMap["RequestTime"].S);
                    r.RideId = attributeMap["RideId"].S;
                    r.UserName = attributeMap["User"].S;
                    r.UnicornName = attributeMap["UnicornName"].S;
                    r.UserName = attributeMap["User"].S;
                    var unicorn = attributeMap["Unicorn"].M;
                    r.Gender = unicorn["Gender"].S;
                    r.Color = unicorn["Color"].S;

                        

                    recordsToPersist.Add(r);
                }
            }
            foreach (var ride in recordsToPersist)
            {
                PersistRecord(ride, connectionString);
            }
            

            Console.WriteLine(".NET Log - Stream processing complete.");
        }

        private void PersistRecord(Ride ride, string connectionString)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))

                {
                   
                    connection.Open();
                    string hackInsert = "INSERT INTO dbo.Rides (RideId, RequestTime, UnicornName, UserName, Color, Gender) VALUES (@RideId, @RequestTime, @UnicornName, @UserName, @Color, @Gender);";



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
                        command.Parameters["@Color"].Value = ride?.RequestTime;

                        command.Parameters.Add("@Gender", SqlDbType.VarChar);
                        command.Parameters["@Gender"].Value = ride?.Gender;

                        command.ExecuteNonQuery();
                        
                      
                    }
                }



            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception " + ex.Message);
            }
           
        }

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
