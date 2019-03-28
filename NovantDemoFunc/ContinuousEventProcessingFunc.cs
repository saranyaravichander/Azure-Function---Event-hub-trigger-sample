using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Xml.Serialization;
using System.Xml.Linq;
using Newtonsoft.Json;

namespace NovantDemoFunc
{
    public static class ContinuousEventProcessingFunc
    {
        [FunctionName("ContinuousEventProcessingFunc")]
        [return: EventHub("outgoingmsg", Connection = "eventhubconnection")]
        public static async Task<string> Run(
            [EventHubTrigger("incomingmsg",
            Connection = "eventhubconnection")]
        EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function picked a message: {messageBody}");

                    //// 1. SQL query

                    string valueFromDb = string.Empty;
                    using (SqlConnection connection = new SqlConnection(Environment.GetEnvironmentVariable("sqlConnection")))
                    {
                        SqlCommand command = new SqlCommand("select name from Test where id = @id", connection);
                        command.Parameters.AddWithValue("@id", "1");
                        connection.Open();
                        SqlDataReader reader = command.ExecuteReader();
                        try
                        {
                            while (reader.Read())
                            {
                                valueFromDb = reader["name"].ToString();
                            }
                        }
                        finally
                        {
                            reader.Close();
                        }
                    }

                    //// 2. Call Soap request
                    var soap = @"<?xml version=""1.0"" encoding=""utf-8""?>
                        <soapenv:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:soapenv=""http://schemas.xmlsoap.org/soap/envelope/"">
                          <soapenv:Body>
                            <Product xmlns=""http://xmlns.xyz.com/webservice/version"">
                              <Id>" + messageBody + @"</Id>
                              <Name>some product</Name>
                            </Product>
                          </soapenv:Body>
                        </soapenv:Envelope>";

                    var val = XDocument.Parse(soap);
                    SOAPEnvelope deserializedObject;
                    using (var reader = val.CreateReader(System.Xml.Linq.ReaderOptions.None))
                    {
                        var ser = new XmlSerializer(typeof(SOAPEnvelope));
                        deserializedObject = (SOAPEnvelope)ser.Deserialize(reader);
                    }

                    string jsonString = JsonConvert.SerializeObject(deserializedObject.body);

                    return jsonString;
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();

            return "none";
        }

        [XmlType(Namespace = "http://schemas.xmlsoap.org/soap/envelope/")]
        [XmlRoot(ElementName = "Envelope", Namespace = "http://schemas.xmlsoap.org/soap/envelope/")]
        public class SOAPEnvelope
        {
            [XmlAttribute(AttributeName = "soapenv", Namespace = "http://schemas.xmlsoap.org/soap/envelope/")]
            public string soapenva { get; set; }
            [XmlAttribute(AttributeName = "xsd", Namespace = "http://www.w3.org/2001/XMLSchema")]
            public string xsd { get; set; }
            [XmlAttribute(AttributeName = "xsi", Namespace = "http://www.w3.org/2001/XMLSchema-instance")]
            public string xsi { get; set; }
            [XmlElement(ElementName = "Body", Namespace = "http://schemas.xmlsoap.org/soap/envelope/")]
            public ResponseBody<Product> body { get; set; }
            [XmlNamespaceDeclarations]
            public XmlSerializerNamespaces xmlns = new XmlSerializerNamespaces();
            public SOAPEnvelope()
            {
                xmlns.Add("soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
            }
        }

        [XmlRoot(ElementName = "Body", Namespace = "http://schemas.xmlsoap.org/soap/envelope/")]
        public class ResponseBody<T>
        {
            [XmlElement(Namespace = "http://xmlns.xyz.com/webservice/version")]
            public T Product { get; set; }
        }

        public class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
