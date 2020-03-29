// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Reflection;
using System.Collections.Generic;
using SendGrid;
using SendGrid.Helpers;
using SendGrid.Helpers.Mail;

namespace payerfiletrigger
{
    public static class payerfiletrigger
    {
        private static System.Data.DataTable dtPayerDataTable;
        [FunctionName("payerfiletrigger")]
        public static void Run([EventGridTrigger]EventGridEvent eventGridEvent,ILogger log)
        {
            System.Text.StringBuilder ErrorRecords = new System.Text.StringBuilder();
            System.Text.StringBuilder AcceptedRecords = new System.Text.StringBuilder();
            RootObject rootObject = Newtonsoft.Json.JsonConvert.DeserializeObject<RootObject>(eventGridEvent.Data.ToString());
            if (rootObject.url.Contains("/LandBlob/"))            
            {
                string blobName = rootObject.url.Substring(rootObject.url.IndexOf("/claimshark/")).Replace("/claimshark/", "");
                string PayerID = blobName.Split("/")[0];
                if (!CheckFileExists(PayerID, System.IO.Path.GetFileName(blobName)))
                {
                    CloudStorageAccount mycloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnection"));
                    CloudBlobClient blobClient = mycloudStorageAccount.CreateCloudBlobClient();
                    CloudBlobContainer container = blobClient.GetContainerReference("claimshark");
                    CloudBlob cloudBlockBlob = container.GetBlobReference(blobName);
                    string text;
                    List<FileError> fileErrors = new List<FileError>();
                    using (var stream = new System.IO.MemoryStream())
                    {
                        cloudBlockBlob.DownloadToStreamAsync(stream).Wait();
                        text = System.Text.Encoding.UTF8.GetString(stream.ToArray());
                    }
                    if (!String.IsNullOrEmpty(text))
                    {
                        var binDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                        string PayerSchemaFile = Path.Combine(binDirectory, PayerID + ".json");
                        string PayerSchema;
                        if (System.IO.File.Exists(PayerSchemaFile))
                            PayerSchema = File.ReadAllText(PayerSchemaFile);
                        else
                        {
                            binDirectory = binDirectory.Substring(0, binDirectory.LastIndexOf(@"\"));
                            PayerSchemaFile = Path.Combine(binDirectory, PayerID + ".json");
                            PayerSchema = File.ReadAllText(PayerSchemaFile);
                        }
                        FileSchema fileSchema = Newtonsoft.Json.JsonConvert.DeserializeObject<FileSchema>(PayerSchema);
                        CreateTable(fileSchema.Fileds);
                        string[] data = text.Split('\n');
                        int LineNo = 1;
                        foreach (var value in data)
                        {
                            if (value.Trim().Length > 0)
                            {
                                System.Data.DataRow dataRow = dtPayerDataTable.NewRow();
                                dataRow["IsValid"] = 1;
                                if (fileSchema.FieldSeprator == "FixedLength")
                                {
                                    int startPosition = 1;
                                    int fieldLength = 0;
                                    foreach (var field in fileSchema.Fileds)
                                    {
                                        fieldLength = Convert.ToInt32(field.Length);
                                        if (value.Length >= (startPosition + fieldLength))
                                        {
                                            dataRow[field.FieldName] = value.Substring(startPosition - 1, fieldLength);
                                            if (field.IsRequired == "Yes")
                                            {
                                                if (value.Substring(startPosition - 1, fieldLength).Trim().Length <= 0)
                                                {
                                                    fileErrors.Add(new FileError()
                                                    {
                                                        FieldName = field.FieldName,
                                                        StartingPostiton = startPosition.ToString(),
                                                        FileLinePosition = LineNo.ToString()
                                                    });
                                                    dataRow["IsValid"] = 0;
                                                }                                                
                                            }
                                        }
                                        else
                                        {
                                            if (value.Length > startPosition)
                                            {
                                                dataRow[field.FieldName] = value.Substring(startPosition - 1);
                                                if (field.IsRequired == "Yes")
                                                {
                                                    if (value.Substring(startPosition - 1).Trim().Length <= 0)
                                                    {
                                                        fileErrors.Add(new FileError()
                                                        {
                                                            FieldName = field.FieldName,
                                                            StartingPostiton = startPosition.ToString(),
                                                            FileLinePosition = LineNo.ToString()
                                                        });
                                                        dataRow["IsValid"] = 0;
                                                    }                                                    
                                                }
                                                break;
                                            }
                                        }
                                        startPosition += fieldLength;
                                    }
                                    dtPayerDataTable.Rows.Add(dataRow);
                                }
                            }
                            if (fileErrors.Count > 0)
                                ErrorRecords.Append(value);
                            else
                                AcceptedRecords.Append(value);
                            LineNo += 1;
                        }
                    }
                    LogFileDetails(fileErrors, blobName,PayerID);
                    UploadBlob(fileErrors,ErrorRecords, AcceptedRecords, blobName, PayerID);
                    SendEmail(blobName, PayerID);
                    log.LogInformation(Newtonsoft.Json.JsonConvert.SerializeObject(fileErrors));
                }
            }
        }
        private static void CreateTable(List<Field> fields)
        {
            dtPayerDataTable = new System.Data.DataTable();
            string strDataType = "System.";
            foreach(var field in fields)
            {
                strDataType = "System.";
                switch (field.DataType.ToLower())
                {
                    case "string":
                        strDataType = strDataType + "String";
                        break;
                    case "int":
                        strDataType = strDataType + "Int32";
                        break;
                    case "decimal":
                        strDataType = strDataType + "decimal";
                        break;

                } 
                dtPayerDataTable.Columns.Add(new System.Data.DataColumn(field.FieldName, Type.GetType(strDataType)));
            }
            dtPayerDataTable.Columns.Add(new System.Data.DataColumn("IsValid", Type.GetType("System.Int32")));

        }
        private static void LogFileDetails(List<FileError> fileErrors, string blobName, string PayerID)
        {
            string FileName = System.IO.Path.GetFileName(blobName);
            FileDetails fileDetails = new FileDetails()
            {
                PayerID = PayerID,
                TotalRecords = dtPayerDataTable.Rows.Count.ToString(),
                AcceptedRecords = (dtPayerDataTable.Rows.Count - fileErrors.Count).ToString(),
                ErrorRecords = fileErrors.Count.ToString(),
                FileName= FileName
            };
            CloudStorageAccount mycloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnection"));
            CloudBlobClient blobClient = mycloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("claimshark");
            container.CreateIfNotExistsAsync().Wait();
            var blob = container.GetBlockBlobReference(PayerID +"/FileRegister/Details_" + FileName);
            using (Stream stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(fileDetails))))
            {
                blob.UploadFromStreamAsync(stream).Wait();
            }
        }
        private static void UploadBlob(List<FileError> fileErrors,System.Text.StringBuilder ErrorRecord, System.Text.StringBuilder AcceptedRecord,string blobName, string PayerID)
        {
            string FileName = System.IO.Path.GetFileName(blobName);
            CloudStorageAccount mycloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnection"));
            CloudBlobClient blobClient = mycloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("claimshark");
            container.CreateIfNotExistsAsync().Wait();
            if (ErrorRecord.ToString().Length > 0)
            {
                var blob = container.GetBlockBlobReference(PayerID + "/ErrorBlob/" + FileName);
                using (Stream stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ErrorRecord.ToString())))
                {
                    blob.UploadFromStreamAsync(stream).Wait();                   
                }
                blob = container.GetBlockBlobReference(PayerID + "/ErrorBlob/ErrorDetails_" + FileName);
                using (Stream stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(fileErrors))))
                {
                    blob.UploadFromStreamAsync(stream).Wait();
                }
            }
            if (AcceptedRecord.ToString().Length > 0)
            {
                System.Data.DataTable dataTableClean = dtPayerDataTable.Clone();
                var Accepted =dtPayerDataTable.Select("IsValid=1");
                foreach (var dr in Accepted)
                {
                    dataTableClean.ImportRow(dr);
                }
                var blob = container.GetBlockBlobReference(PayerID + "/Payer/InBound/" + FileName);
                using (Stream stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(dataTableClean))))
                {
                    blob.UploadFromStreamAsync(stream).Wait();
                }
            }

        }
        private static void SendEmail(string blobName,string PayerID)
        {
            string FileName = System.IO.Path.GetFileName(blobName);
            string text = string.Empty;
            CloudStorageAccount mycloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnection"));
            CloudBlobClient blobClient = mycloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("claimshark");
            var blob = container.GetBlockBlobReference(PayerID + "/FileRegister/Details_" + FileName);
            using (var stream = new System.IO.MemoryStream())
            {
                blob.DownloadToStreamAsync(stream).Wait();
                text = System.Text.Encoding.UTF8.GetString(stream.ToArray());
            }
            if (!String.IsNullOrEmpty(text))
            {
                FileDetails fileDetails = JsonConvert.DeserializeObject<FileDetails>(text);

                string msgSring = "<table> <tr>";
                   msgSring+= "<td> Payer ID : </td> <td> <strong> " + fileDetails.PayerID + "</strong> </td> </tr>";
                msgSring += "<tr> <td> File Name : </td> <td> <strong> " + fileDetails.FileName + "</strong> </td> </tr>";
                msgSring += "<tr> <td> AcceptedRecords : </td> <td> <strong> " + fileDetails.AcceptedRecords + "</strong> </td> </tr>";
                msgSring += "<tr> <td> ErrorRecords : </td> <td> <strong> " + fileDetails.ErrorRecords + "</strong> </td> </tr>";
                msgSring += "<tr> <td> TotalRecords : </td> <td> <strong> " + fileDetails.TotalRecords + "</strong> </td> </tr>";
                msgSring += "</table>";
                string sendGridKey = Environment.GetEnvironmentVariable("SendGridKey");
                SendGrid.SendGridClient sendGridClient = new SendGridClient(sendGridKey);
                var msg = new SendGridMessage()
                {
                    From = new EmailAddress("etl@claimshark.com", "ClaimShark Team"),
                    Subject = "ClaimShark Etl",
                    HtmlContent = msgSring
                };
                msg.AddTo(new EmailAddress("bimal.kurichiyath@ispace.com", "Bimal Kurichiyath"));
                msg.AddTo(new EmailAddress("dinesh.panda@ispace.com", "Dinesh Kumar Panda"));
                msg.AddTo(new EmailAddress("saiganesh.kasina@ispace.com", "Sai Ganesh Swamy Kasina"));
                sendGridClient.SendEmailAsync(msg).Wait();
            }
        }
        private static bool CheckFileExists(string PayerID, string FileName)
        {
            CloudStorageAccount mycloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnection"));
            CloudBlobClient blobClient = mycloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("claimshark");
            //container.CreateIfNotExistsAsync().Wait();
            var blob = container.GetBlockBlobReference(PayerID + "/FileRegister/Details_" + FileName);
            try
            {
                blob.FetchAttributesAsync().Wait();
                return true;
            }
            catch
            {
                return false;
            }
        }



    }
}

