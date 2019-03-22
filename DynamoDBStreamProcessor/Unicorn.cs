using System;
using System.Collections.Generic;
using System.Text;

namespace DynamoDBStreamProcessor
{
    //This is the Unicorn class structure used in the other code (how it is stored in dynamo)
    public class Unicorn
    {
        public string Name { get; set; }

        public string Color { get; set; }

        public string Gender { get; set; }
    }

}
