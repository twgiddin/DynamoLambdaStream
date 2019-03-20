using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDBStreamProcessor
{
    public class Ride
    {
        public string RideId { get; set; }
        public DateTime RequestTime { get; set; }
        public string UnicornName { get; set; }
        public string Gender { get; set; }
        public string Color { get; set; }
        public string UserName { get; set; }
    }

}
