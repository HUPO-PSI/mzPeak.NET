using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using MZPeak.Compute;


namespace MZPeak.Metadata;


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
    List<ColumnParam> spectrumMetadataColumns;
    RecordBatch? scanMetadata = null;
    List<ColumnParam> scanMetadataColumns;
    RecordBatch? precursorMetadata = null;
    List<ColumnParam> precursorMetadataColumns;
    RecordBatch? selectedIonMetadata = null;
    List<ColumnParam> selectedIonMetadataColumns;

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

        spectrumMetadataColumns = new();
        scanMetadataColumns = new();
        precursorMetadataColumns = new();
        selectedIonMetadataColumns = new();

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
            spectrumMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            var specSchema = new Schema(dtype.Fields, []);
            var shard = new RecordBatch(specSchema, spectrumCol.Fields, spectrumCol.Length);
            SpectrumMetadata = shard;
        }
        var scanCol = (StructArray?)batch.Column("scan");
        if (scanCol != null)
        {
            var dtype = (StructType)scanCol.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            scanMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            ScanMetadata = new RecordBatch(specSchema, scanCol.Fields, scanCol.Length);
        }
        var precursor = (StructArray?)batch.Column("precursor");
        if (precursor != null)
        {
            var dtype = (StructType)precursor.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            precursorMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            PrecursorMetadata = new RecordBatch(specSchema, precursor.Fields, precursor.Length);
        }
        var selectedIon = (StructArray?)batch.Column("selected_ion");
        if (selectedIon != null)
        {
            var dtype = (StructType)selectedIon.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            selectedIonMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            SelectedIonMetadata = new RecordBatch(specSchema, selectedIon.Fields, selectedIon.Length);
        }
    }

}

public class ChromatogramMetadataReader : MetadataReaderBase
{
    public ParquetSharp.Arrow.FileReader FileReader;

    RecordBatch? chromatogramMetadata = null;
    List<ColumnParam> chromatogramMetadataColumns;
    RecordBatch? precursorMetadata = null;
    List<ColumnParam> precursorMetadataColumns;
    RecordBatch? selectedIonMetadata = null;
    List<ColumnParam> selectedIonMetadataColumns;

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
        chromatogramMetadataColumns = new();
        precursorMetadataColumns = new();
        selectedIonMetadataColumns = new();
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
            chromatogramMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            var shard = new RecordBatch(specSchema, spectrumCol.Fields, spectrumCol.Length);
            ChromatogramMetadata = shard;
        }
        var precursor = (StructArray?)batch.Column("precursor");
        if (precursor != null)
        {
            var dtype = (StructType)precursor.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            precursorMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            PrecursorMetadata = new RecordBatch(specSchema, precursor.Fields, precursor.Length);
        }
        var selectedIon = (StructArray?)batch.Column("selected_ion");
        if (selectedIon != null)
        {
            var dtype = (StructType)selectedIon.Data.DataType;
            var specSchema = new Schema(dtype.Fields, []);
            selectedIonMetadataColumns = ColumnParam.FromFields(dtype.Fields);
            SelectedIonMetadata = new RecordBatch(specSchema, selectedIon.Fields, selectedIon.Length);
        }
    }
}
