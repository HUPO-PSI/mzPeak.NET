using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Reader;


namespace MZPeak.Metadata;


[JsonConverter(typeof(ParamJsonConverter))]
public class Param {
    public string Name { get; set; }
    public string? AccessionCURIE { get; set; }
    object? rawValue;
    public string? UnitCURIE { get; set; }

    public bool IsDouble()
    {
        return rawValue is double;
    }

    public bool IsLong()
    {

        return rawValue is long;
    }

    public bool IsString()
    {
        return rawValue is string;
    }

    public bool IsBoolean()
    {
        return rawValue is bool;
    }

    public bool IsNull()
    {
        return rawValue == null;
    }

    public Param(string name, object rawValue)
    {
        Name = name;
        this.rawValue = rawValue;
    }

    public Param(string name, string accession, object rawValue)
    {
        Name = name;
        AccessionCURIE = accession;
        this.rawValue = rawValue;
    }

    public Param(string name, string? accession, object? rawValue, string? unit)
    {
        Name = name;
        AccessionCURIE = accession;
        this.rawValue = rawValue;
        UnitCURIE = unit;
    }

    public string AsString()
    {
        var s = Convert.ToString(rawValue);
        return s == null ? "" : s;
    }

    public long AsLong()
    {
        return Convert.ToInt64(rawValue);
    }

    public double AsDouble()
    {
        return Convert.ToDouble(rawValue);
    }

    public bool AsBoolean()
    {
        return Convert.ToBoolean(rawValue);
    }

    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append("Param {\n");
        builder.Append("\tName = ");
        builder.Append(Name);
        builder.Append(",\n\tAccessionCURIE = ");
        builder.Append(AccessionCURIE);
        builder.Append(",\n\trawValue = ");
        builder.Append(rawValue);
        builder.Append(",\n\tUnitCURIE = ");
        builder.Append(UnitCURIE);
        builder.Append("\n}");
        return builder.ToString();
    }
}


public class ParamJsonConverter : JsonConverter<Param>
{
    public override Param? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException();
        }
        string? name = null;
        object? value = null;
        string? accession = null;
        string? unit = null;
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                if (name == null)
                {
                    throw new JsonException("parameter name cannot be null");
                }
                return new Param(name, accession, value, unit);
            }

            // Get the key.
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            string? propertyName = reader.GetString();
            reader.Read();
            switch (propertyName) {
                case null:
                    {
                        throw new JsonException("property name cannot be null");
                    }
                case "name":
                    {
                        name = reader.GetString();
                        if (name == null)
                        {
                            throw new JsonException("parameter name cannot be null");
                        }
                        break;
                    }
                case "value":
                    {
                        if (reader.TokenType == JsonTokenType.Number)
                        {
                            double vdouble;
                            long vlong;
                            if (reader.TryGetDouble(out vdouble))
                            {
                                value = vdouble;
                            } else if (reader.TryGetInt64(out vlong))
                            {
                                value = vlong;
                            }
                        }
                        else if ((reader.TokenType == JsonTokenType.True) || reader.TokenType == JsonTokenType.False)
                        {
                            value = reader.GetBoolean();
                        }
                        else if (reader.TokenType == JsonTokenType.String)
                        {
                            value = reader.GetString();
                        }
                        else if (reader.TokenType == JsonTokenType.Null)
                        {
                            value = null;
                            reader.Skip();
                        }

                        break;
                    }

                case "accession":
                    {
                        accession = reader.GetString();
                        break;
                    }
                case "unit":
                    {
                        unit = reader.GetString();
                        break;
                    }
                default:
                    {
                        break;
                    }
            }
        }
        throw new JsonException("Unclosed parameter object");
    }

    public override void Write(Utf8JsonWriter writer, Param value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        writer.WriteString("name", value.Name);
        writer.WriteString("accession", value.AccessionCURIE);

        if (value.IsBoolean())
        {
            writer.WriteBoolean("value", value.AsBoolean());
        }
        else if (value.IsDouble())
        {
            writer.WriteNumber("value", value.AsDouble());
        }
        else if (value.IsLong())
        {
            writer.WriteNumber("value", value.AsLong());
        }
        else if (value.IsString())
        {
            writer.WriteString("value", value.AsString());
        } else if (value.IsNull())
        {
            writer.WriteNull("value");
        }

        writer.WriteString("unit", value.UnitCURIE);
        writer.WriteEndObject();
    }
}


/// <summary>
/// A base class for generic metadata table reading
/// </summary>
public class MetadataReaderBase
{
    protected MzPeakMetadata mzPeakMetadata;

    public FileDescription FileDescription => mzPeakMetadata.FileDescription;
    public List<InstrumentConfiguration> InstrumentConfigurations => mzPeakMetadata.InstrumentConfigurations;
    public List<Software> Softwares => mzPeakMetadata.Softwares;
    public List<Sample> Samples => mzPeakMetadata.Samples;
    public List<DataProcessingMethod> DataProcessingMethods => mzPeakMetadata.DataProcessingMethods;

    protected MetadataReaderBase(MzPeakMetadata mzPeakMetadata)
    {
        this.mzPeakMetadata = mzPeakMetadata;
    }

    protected Dictionary<ulong, string?> GetNativeIdsFrom(RecordBatch? table)
    {
        if (table == null)
        {
            return new();
        }
        var fieldIdx = table.Schema.GetFieldIndex("id");
        if (fieldIdx < 0)
        {
            return new();
        }

        var indexArr = (UInt64Array)table.Column(0);
        var modelArr = (LargeStringArray)table.Column(fieldIdx);
        Dictionary<ulong, string?> nativeIds = new();
        nativeIds.EnsureCapacity(indexArr.Length);
        for (var i = 0; i < indexArr.Length; i++)
        {
            var index = indexArr.GetValue(i);
            if (index == null)
            {
                continue;
            }
            var nativeId = modelArr.GetString(i);
            nativeIds.Add((ulong)index, nativeId);
        }
        return nativeIds;
    }

}


public class SpectrumMetadataReader : MetadataReaderBase
{
    public ParquetSharp.Arrow.FileReader FileReader;

    RecordBatch? spectrumMetadata = null;
    RecordBatch? scanMetadata = null;
    RecordBatch? precursorMetadata = null;
    RecordBatch? selectedIonMetadata = null;

    public int Length { get
        {
            if (SpectrumMetadata == null) {
                InitializeTables().Wait();
            }
            return SpectrumMetadata == null ? 0 : SpectrumMetadata.Length;
        }
    }

    public SpectrumMetadataReader(ParquetSharp.Arrow.FileReader fileReader, bool initializeFacets=true) : base(MzPeakMetadata.FromParquet(fileReader.ParquetReader))
    {
        FileReader = fileReader;
        if (initializeFacets)
        {
            InitializeTables().Wait();
        }
    }

    public Dictionary<ulong, SpacingInterpolationModel<double>> GetSpacingModelIndex()
    {
        if (SpectrumMetadata == null)
        {
            return new();
        }
        var fieldIdx = SpectrumMetadata.Schema.GetFieldIndex("mz_delta_model");
        if(fieldIdx < 0)
        {
            return new();
        }

        var indexArr = (UInt64Array)SpectrumMetadata.Column(0);
        var modelArr = (LargeListArray)SpectrumMetadata.Column(fieldIdx);
        Dictionary<ulong, SpacingInterpolationModel<double>> accumulator = new();
        for(var i = 0; i < indexArr.Length; i++)
        {
            var index = indexArr.GetValue(i);
            if (index == null)
            {
                continue;
            }
            if (modelArr.IsNull(i))
            {
                continue;
            }
            var modelAt = modelArr.GetSlicedValues(i);
            var coefs = new List<double>();
            switch(modelAt.Data.DataType.TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        foreach (var v in (FloatArray)modelAt)
                        {
                            if (v != null)
                            {
                                coefs.Add((double)v);
                            }
                        }
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        foreach (var v in (DoubleArray)modelAt)
                        {
                            if (v != null)
                            {
                                coefs.Add((double)v);
                            }
                        }
                        break;
                    }
                default:
                    {
                        throw new InvalidDataException("Only float and double arrays are supported in mz_delta_model");
                    }
            }
            if(coefs.Count > 0)
            {
                accumulator[(ulong)index] = new SpacingInterpolationModel<double>(coefs);
            }
        }
        return accumulator;
    }
    public Dictionary<ulong, string?> GetNativeIds()
    {
        return GetNativeIdsFrom(SpectrumMetadata);
    }

    public RecordBatch? SpectrumMetadata { get {
        if (spectrumMetadata == null)
            {
                InitializeTables().Wait();
            }
        return spectrumMetadata;
    } set => spectrumMetadata = value; }
    public RecordBatch? ScanMetadata { get {
            if (scanMetadata == null)
            {
                InitializeTables().Wait();
            }
            return scanMetadata;
        } set => scanMetadata = value; }
    public RecordBatch? PrecursorMetadata
    {
        get
        {
            if (precursorMetadata == null)
            {
                InitializeTables().Wait();
            }
            return precursorMetadata;
        }
        set => precursorMetadata = value;
    }
    public RecordBatch? SelectedIonMetadata
    {
        get
        {
            if (selectedIonMetadata == null)
            {
                InitializeTables().Wait();
            }
            return selectedIonMetadata;
        }
        set => selectedIonMetadata = value;
    }

    public async Task InitializeTables()
    {
        var reader = FileReader.GetRecordBatchReader();
        var builder = new RecordBatch.Builder();
        RecordBatch batch;
        while (true) {
            batch = await reader.ReadNextRecordBatchAsync();
            if(batch == null)
            {
                break;
            }
            builder.Append(batch);
        }
        batch = builder.Build();
        var spectrumCol = (StructArray?)batch.Column("spectrum");
        if (spectrumCol != null)
        {

            var dtype = (StructType)spectrumCol.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            var shard = new RecordBatch(specSchema, spectrumCol.Fields, spectrumCol.Length);
            SpectrumMetadata = shard;
        }
        var scanCol = (StructArray?)batch.Column("scan");
        if (scanCol != null)
        {
            var dtype = (StructType)scanCol.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            ScanMetadata = new RecordBatch(specSchema, scanCol.Fields, scanCol.Length);
        }
        var precursor = (StructArray?)batch.Column("precursor");
        if (precursor != null)
        {
            var dtype = (StructType)precursor.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            PrecursorMetadata = new RecordBatch(specSchema, precursor.Fields, precursor.Length);
        }
        var selectedIon = (StructArray?)batch.Column("selected_ion");
        if (selectedIon != null)
        {
            var dtype = (StructType)selectedIon.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            SelectedIonMetadata = new RecordBatch(specSchema, selectedIon.Fields, selectedIon.Length);
        }
    }

}

public class ChromatogramMetadataReader : MetadataReaderBase
{
    public ParquetSharp.Arrow.FileReader FileReader;

    RecordBatch? chromatogramMetadata = null;
    RecordBatch? precursorMetadata = null;
    RecordBatch? selectedIonMetadata = null;

    public int Length
    {
        get
        {
            if (ChromatogramMetadata == null)
            {
                InitializeTables().Wait();
            }
            return ChromatogramMetadata == null ? 0 : ChromatogramMetadata.Length;
        }
    }

    public ChromatogramMetadataReader(ParquetSharp.Arrow.FileReader fileReader, bool initializeFacets = true) : base(MzPeakMetadata.FromParquet(fileReader.ParquetReader))
    {
        FileReader = fileReader;
        if (initializeFacets)
        {
            InitializeTables().Wait();
        }
    }

    public RecordBatch? ChromatogramMetadata
    {
        get
        {
            if (chromatogramMetadata == null)
            {
                InitializeTables().Wait();
            }
            return chromatogramMetadata;
        }
        set => chromatogramMetadata = value;
    }

    public RecordBatch? PrecursorMetadata
    {
        get
        {
            if (precursorMetadata == null)
            {
                InitializeTables().Wait();
            }
            return precursorMetadata;
        }
        set => precursorMetadata = value;
    }

    public RecordBatch? SelectedIonMetadata
    {
        get
        {
            if (selectedIonMetadata == null)
            {
                InitializeTables().Wait();
            }
            return selectedIonMetadata;
        }
        set => selectedIonMetadata = value;
    }

    public Dictionary<ulong, string?> GetNativeIds()
    {
        return GetNativeIdsFrom(ChromatogramMetadata);
    }

    public async Task InitializeTables()
    {
        var reader = FileReader.GetRecordBatchReader();
        var builder = new RecordBatch.Builder();
        RecordBatch batch;
        while (true)
        {
            batch = await reader.ReadNextRecordBatchAsync();
            if (batch == null)
            {
                break;
            }
            builder.Append(batch);
        }
        batch = builder.Build();
        var spectrumCol = (StructArray?)batch.Column("chromatogram");
        if (spectrumCol != null)
        {

            var dtype = (StructType)spectrumCol.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            var shard = new RecordBatch(specSchema, spectrumCol.Fields, spectrumCol.Length);
            ChromatogramMetadata = shard;
        }
        var precursor = (StructArray?)batch.Column("precursor");
        if (precursor != null)
        {
            var dtype = (StructType)precursor.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            PrecursorMetadata = new RecordBatch(specSchema, precursor.Fields, precursor.Length);
        }
        var selectedIon = (StructArray?)batch.Column("selected_ion");
        if (selectedIon != null)
        {
            var dtype = (StructType)selectedIon.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            SelectedIonMetadata = new RecordBatch(specSchema, selectedIon.Fields, selectedIon.Length);
        }
    }
}
