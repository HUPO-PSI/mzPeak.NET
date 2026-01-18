namespace MZPeak.Metadata;

using System.Text.RegularExpressions;
using System.Text.Json.Serialization;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BufferFormat
{
    [JsonStringEnumMemberName("point")]
    Point,
    [JsonStringEnumMemberName("chunk_values")]
    ChunkValues,
    [JsonStringEnumMemberName("chunk_start")]
    ChunkStart,
    [JsonStringEnumMemberName("chunk_end")]
    ChunkEnd,
    [JsonStringEnumMemberName("chunk_encoding")]
    ChunkEncoding,
    [JsonStringEnumMemberName("chunk_secondary")]
    ChunkSecondary,
    [JsonStringEnumMemberName("chunk_transform")]
    ChunkTransform,
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BufferPriority
{
    [JsonStringEnumMemberName("primary")]
    Primary,
    [JsonStringEnumMemberName("secondary")]
    Secondary
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BufferContext
{
    [JsonStringEnumMemberName("spectrum")]
    Spectrum,
    [JsonStringEnumMemberName("chromatogram")]
    Chromatogram,
}

static class BufferContexteMethods
{
    public static string IndexName(this BufferContext entityType)
    {
        switch (entityType)
        {
            case BufferContext.Spectrum:
                {
                    return "spectrum_index";
                }
            case BufferContext.Chromatogram:
                {
                    return "chromatogram_index";
                }
            default:
                {
                    throw new InvalidOperationException("Cannot create index column name for `Other`");
                }
        }
    }

    public static ArrayType DefaultPrimaryAxis(this BufferContext entityType)
    {
        switch(entityType)
        {
            case BufferContext.Spectrum:
                {
                    return ArrayType.MZArray;
                }
            case BufferContext.Chromatogram:
                {
                    return ArrayType.TimeArray;
                }
            default: throw new InvalidOperationException("Unknown axis type for `Other`");
        }
    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record ArrayIndexEntry
{
    [JsonPropertyName("context")]
    public required BufferContext Context { get; set; }

    [JsonPropertyName("path")]
    public required string Path { get; set; }

    [JsonPropertyName("data_type")]
    public required string DataTypeCURIE { get; set; }

    [JsonPropertyName("array_type")]
    public required string ArrayTypeCURIE { get; set; }

    [JsonPropertyName("array_name")]
    public required string ArrayName { get; set; }

    [JsonPropertyName("unit")]
    public string? UnitCURIE { get; set; } = null;

    [JsonPropertyName("transform")]
    public string? Transform { get; set; } = null;

    [JsonPropertyName("buffer_format")]
    public BufferFormat BufferFormat { get; set; }

    [JsonPropertyName("data_processing_id")]
    public string? DataProcessesingId { get; set; } = null;

    [JsonPropertyName("buffer_priority")]
    public BufferPriority? BufferPriority { get; set; } = null;

    [JsonPropertyName("sorting_rank")]
    public uint? SortingRank { get; set; } = null;

    [JsonIgnore]
    public int? SchemaIndex { get; set; } = null;

    public ArrowType GetArrowType()
    {
        switch (DataTypeCURIE)
        {
            case "MS:1000521":
                {
                    return new FloatType();
                }
            case "MS:1000519":
                {
                    return new Int32Type();
                }
            case "MS:1000522":
                {
                    return new Int64Type();
                }
            case "MS:1000523":
                {
                    return new DoubleType();
                }
            case "MS:1001479":
                {
                    return new LargeStringType();
                }
            default:
                {
                    throw new InvalidDataException("Cannot map " + DataTypeCURIE + " to an Arrow type");
                }
        }
    }

    public string CreateColumnName()
    {
        var notAlpha = new Regex("[^A-Za-z_]+");
        if (BufferPriority == Metadata.BufferPriority.Primary)
        {
            return notAlpha.Replace(ArrayName.Replace("m/z", "mz").Replace(" array", ""), "_");
        } else
        {
            throw new NotImplementedException("TODO");
        }
    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public class ArrayIndex
{
    [JsonPropertyName("prefix")]
    public required string Prefix { get; set; }

    [JsonPropertyName("entries")]
    public required List<ArrayIndexEntry> Entries { get; set; }

    public int Length { get => Entries.Count; }
}