using System;
using System.Collections.Generic;
using System.Text;

namespace payerfiletrigger
{
    public class FileError
    {
        public string FieldName { get; set; }
        public string StartingPostiton { get; set; }
        public string FileLinePosition { get; set; }
    }

    public class FileDetails
    {
        public string PayerID { get; set; }
        public string FileName { get; set; }
        public string ErrorRecords { get; set; }
        public string AcceptedRecords { get; set; }
        public string TotalRecords { get; set; }
    }
}
