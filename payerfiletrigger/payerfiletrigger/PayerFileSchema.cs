using System.Collections.Generic;

public class Field
{
    public string FieldName { get; set; }
    public string DataType { get; set; }
    public string StartingPosition { get; set; }
    public string Length { get; set; }
    public string IsRequired { get; set; }
}

public class FileSchema
{
    public string FieldSeprator { get; set; }
    public List<Field> Fileds { get; set; }
}