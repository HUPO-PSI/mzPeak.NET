namespace MZPeak.Metadata;

using System.Text.Json;
using System.Text.Json.Serialization;
using MZPeak.ControlledVocabulary;
using ParquetSharp;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record FileDescription
{
    public FileDescription()
    {
        Contents = new();
        SourceFiles = new();
    }

    public static FileDescription Empty()
    {
        return new FileDescription
        {
            Contents = new(),
            SourceFiles = new(),
        };
    }

    [JsonPropertyName("contents")]
    public List<Param> Contents {get; set;}
    [JsonPropertyName("source_files")]
    public List<SourceFile> SourceFiles {get; set;}
}


[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record SourceFile
{
    public SourceFile(string id, string name, string location, List<Param> parameters)
    {
        Id = id;
        Name = name;
        Location = location;
        Parameters = parameters;
    }

    [JsonPropertyName("id")]
    public required string Id {get; set;}
    [JsonPropertyName("name")]
    public required string Name {get; set;}
    [JsonPropertyName("location")]
    public required string Location {get; set;}
    [JsonPropertyName("parameters")]
    public required List<Param> Parameters {get; set;}
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ComponentType
{
    [JsonStringEnumMemberName("ionsource")]
    IonSouce,
    [JsonStringEnumMemberName("analyzer")]
    Analyzer,
    [JsonStringEnumMemberName("detector")]
    Detector,
}


[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record InstrumentComponent
{
    [JsonPropertyName("component_type")]
    public required ComponentType ComponentType { get; set; }
    [JsonPropertyName("order")]
    public required int Order { get; set; }
    [JsonPropertyName("parameters")]
    public required List<Param> Parameters { get; set; }
}


[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record InstrumentConfiguration
{
    [JsonPropertyName("id")]
    public required uint Id { get; set; }
    [JsonPropertyName("components")]
    public required List<InstrumentComponent> Components { get; set; }
    [JsonPropertyName("software_reference")]
    public string? SoftwareReference { get; set; }
    [JsonPropertyName("parameters")]
    public required List<Param> Parameters { get; set; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record Software
{
    [JsonPropertyName("id")]
    public required string Id {get; set;}
    [JsonPropertyName("version")]
    public required string Version {get; set;}
    [JsonPropertyName("parameters")]
    public required List<Param> Parameters { get; set; }
}

public record Sample
{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    [JsonPropertyName("name")]
    public required string Name { get; set; }
    [JsonPropertyName("parameters")]
    public required List<Param> Parameters { get; set; }
}


public record ProcessingMethod
{
    [JsonPropertyName("order")]
    public required uint Order { get; set; }
    [JsonPropertyName("software_reference")]
    public required string SoftwareReference { get; set; }

    [JsonPropertyName("parameters")]
    public required List<Param> Parameters { get; set; }
}


public record DataProcessingMethod
{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    [JsonPropertyName("methods")]
    public required List<ProcessingMethod> ProcessingMethods { get; set; }
}


public class MzPeakMetadata
{
    public FileDescription FileDescription { get; set; }
    public List<InstrumentConfiguration> InstrumentConfigurations { get; set; }
    public List<Software> Softwares {get; set;}
    public List<Sample> Samples {get; set;}
    public List<DataProcessingMethod> DataProcessingMethods { get; set; }

    public MzPeakMetadata()
    {
        FileDescription = new FileDescription
        {
            Contents = new(),
            SourceFiles = new()
        };
        InstrumentConfigurations = new();
        Softwares = new();
        Samples = new();
        DataProcessingMethods = new();
    }

    public MzPeakMetadata(FileDescription description, List<InstrumentConfiguration> instrumentConfigurations, List<Software> softwares, List<Sample> samples, List<DataProcessingMethod> dataProcessingMethods)
    {
        FileDescription = description;
        InstrumentConfigurations = instrumentConfigurations;
        Softwares = softwares;
        Samples = samples;
        DataProcessingMethods = dataProcessingMethods;
    }

    public static MzPeakMetadata FromParquet(ParquetFileReader reader)
    {
        var meta = reader.FileMetaData.KeyValueMetadata;
        string? buf = "";
        FileDescription? fileDescription = new FileDescription();
        if (meta.TryGetValue("file_description", out buf))
        {
            fileDescription = JsonSerializer.Deserialize<FileDescription>(buf);
            if (fileDescription == null) throw new InvalidDataException("file_description failed to deserialize");
        }

        List<InstrumentConfiguration>? instrumentConfigurations = new();
        if (meta.TryGetValue("instrument_configuration_list", out buf))
        {
            instrumentConfigurations = JsonSerializer.Deserialize<List<InstrumentConfiguration>>(buf);
            if(instrumentConfigurations == null) throw new InvalidDataException("instrument_configuration_list failed to deserialize");
        }

        List<Software>? softwares = new();
        if (meta.TryGetValue("software_list", out buf))
        {
            softwares = JsonSerializer.Deserialize<List<Software>>(buf);
            if (softwares == null) throw new InvalidDataException("software_list failed to deserialize");
        }

        List<Sample>? samples = new();
        if (meta.TryGetValue("sample_list", out buf))
        {
            samples = JsonSerializer.Deserialize<List<Sample>>(buf) ?? new();
            if (samples == null) throw new InvalidDataException("sample_list failed to deserialize");
        }

        List<DataProcessingMethod> dataProcessingMethods = new();
        if (meta.TryGetValue("data_processing_method_list", out buf))
        {
            dataProcessingMethods = JsonSerializer.Deserialize<List<DataProcessingMethod>>(buf) ?? new();
            if (dataProcessingMethods == null) throw new InvalidDataException("data_processing_method_list failed to deserialize");
        }

        return new MzPeakMetadata(fileDescription, instrumentConfigurations, softwares, samples, dataProcessingMethods);
    }
}