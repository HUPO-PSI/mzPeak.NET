using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using MZPeak.Compute;
using MZPeak.Reader.Visitors;
using Microsoft.Extensions.Logging;


namespace MZPeak.Metadata;


/// <summary>
/// A base class for generic metadata table reading
/// </summary>
public abstract class MetadataReaderBase<T>
{
    internal static ILogger? Logger = null;

    protected MzPeakMetadata mzPeakMetadata;

    public FileDescription FileDescription => mzPeakMetadata.FileDescription;
    public List<InstrumentConfiguration> InstrumentConfigurations => mzPeakMetadata.InstrumentConfigurations;
    public List<Software> Softwares => mzPeakMetadata.Softwares;
    public List<Sample> Samples => mzPeakMetadata.Samples;
    public List<DataProcessingMethod> DataProcessingMethods => mzPeakMetadata.DataProcessingMethods;
    public MSRun Run => mzPeakMetadata.Run;

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

    public abstract int Length {get;}

    public abstract List<T> BulkLoad();
    public abstract T Get(ulong index);
}


public class SpectrumMetadataReader : MetadataReaderBase<SpectrumDescription>
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

    public override int Length { get
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

    Dictionary<ulong, SpacingInterpolationModel<double>> loadSpectrumInterpolationModels(ListArray modelArr, UInt64Array indexArr)
    {
        Dictionary<ulong, SpacingInterpolationModel<double>> accumulator = new();
        for (var i = 0; i < indexArr.Length; i++)
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
            var coefs = SpacingInterpolationModel<double>.FromArray(modelAt);
            if (coefs != null)
            {
                accumulator[(ulong)index] = coefs;
            }
        }
        return accumulator;
    }

    Dictionary<ulong, SpacingInterpolationModel<double>> loadSpectrumInterpolationModels(LargeListArray modelArr, UInt64Array indexArr)
    {
        Dictionary<ulong, SpacingInterpolationModel<double>> accumulator = new();
        for (var i = 0; i < indexArr.Length; i++)
        {
            var index = indexArr.GetValue(i);
            if (index == null)
            {
                continue;
            }
            if (modelArr.IsNull(i) || modelArr.GetValueLength(i) == 0)
            {
                continue;
            }
            var modelAt = modelArr.GetSlicedValues(i);
            var coefs = SpacingInterpolationModel<double>.FromArray(modelAt);
            if (coefs != null)
            {
                accumulator[(ulong)index] = coefs;
            }
        }
        return accumulator;
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
        var modelArr = SpectrumMetadata.Column(fieldIdx);
        if (modelArr.Data.DataType.TypeId == ArrowTypeId.List)
        {
            return loadSpectrumInterpolationModels((ListArray)modelArr, indexArr);
        }
        else if (modelArr.Data.DataType.TypeId == ArrowTypeId.LargeList)
        {
            return loadSpectrumInterpolationModels((LargeListArray)modelArr, indexArr);
        } else
        {
            throw new NotImplementedException($"{modelArr.Data.DataType.Name} not supported");
        }
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

    public override List<SpectrumDescription> BulkLoad()
    {
        if (SpectrumMetadata == null) return new();
        var spectra = new SpectrumVisitor();
        spectra.Visit(SpectrumMetadata);
        var descrs = spectra.Values.Select(s => new SpectrumDescription(s, new(), new(), new())).ToList();
        if (ScanMetadata != null)
        {
            var visitor = new ScanVisitor();
            visitor.Visit(ScanMetadata);
            foreach(var rec in visitor.Values)
            {
                descrs[(int)rec.SourceIndex].Scans.Add(rec);
            }
        }
        if (PrecursorMetadata != null)
        {
            var visitor = new PrecursorVisitor();
            visitor.Visit(PrecursorMetadata);
            foreach (var rec in visitor.Values)
            {
                descrs[(int)rec.SourceIndex].Precursors.Add(rec);
            }
        }
        if (SelectedIonMetadata != null)
        {
            var visitor = new SelectedIonVisitor();
            visitor.Visit(SelectedIonMetadata);
            foreach (var rec in visitor.Values)
            {
                descrs[(int)rec.SourceIndex].SelectedIons.Add(rec);
            }
        }
        return descrs;
    }

    SpectrumDescription GetSpectrum(ulong index)
    {
        if (SpectrumMetadata == null) throw new IndexOutOfRangeException($"{index} out of spectrum index range");

        var idxArr = (UInt64Array)SpectrumMetadata.Column(0);
        var mask = Compute.Compute.Equal(idxArr, index);
        var recs = Compute.Compute.Filter(SpectrumMetadata, mask);
        var visitor = new SpectrumVisitor();
        visitor.Visit(recs);
        var rec = visitor.Values[0];
        var pn = rec.Parameters.Find(p => p.AccessionCURIE == "MS:1000127");
        List<ScanInfo> scanRecs = new();
        if (ScanMetadata != null)
        {
            idxArr = (UInt64Array)ScanMetadata.Column(0);
            mask = Compute.Compute.Equal(idxArr, index);
            recs = Compute.Compute.Filter(ScanMetadata, mask);
            var scanVisitor = new ScanVisitor();
            scanVisitor.Visit(recs);
            scanRecs = scanVisitor.Values;
        }
        List<PrecursorInfo> precursorInfos = new();
        if (PrecursorMetadata != null)
        {
            idxArr = (UInt64Array)PrecursorMetadata.Column(0);
            mask = Compute.Compute.Equal(idxArr, index);
            recs = Compute.Compute.Filter(PrecursorMetadata, mask);
            var scanVisitor = new PrecursorVisitor();
            scanVisitor.Visit(recs);
            precursorInfos = scanVisitor.Values;
        }
        List<SelectedIonInfo> selectedIons = new();
        if (SelectedIonMetadata != null)
        {
            idxArr = (UInt64Array)SelectedIonMetadata.Column(0);
            mask = Compute.Compute.Equal(idxArr, index);
            recs = Compute.Compute.Filter(SelectedIonMetadata, mask);
            var scanVisitor = new SelectedIonVisitor();
            scanVisitor.Visit(recs);
            selectedIons = scanVisitor.Values;
        }

        return new SpectrumDescription(rec, scanRecs, precursorInfos, selectedIons);
    }

    public async Task InitializeTables()
    {
        var reader = FileReader.GetRecordBatchReader();
        int ctr = 0;
        List<IArrowArray> spectra = [];
        List<IArrowArray> scans = [];
        List<IArrowArray> precursors = [];
        List<IArrowArray> selectedIons = [];
        while (true) {
            RecordBatch batch = await reader.ReadNextRecordBatchAsync();
            if(batch == null)
            {
                Logger?.LogInformation($"Read {ctr} batches from {this}");
                break;
            }
            Logger?.LogInformation("batch {ctr}, {batch.Length} items", batch, ctr);
            ctr++;
            var arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("spectrum"));
            if (arr != null) spectra.Add(arr);
            arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("scan"));
            if (arr != null) scans.Add(arr);
            arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("precursor"));
            if (arr != null) precursors.Add(arr);
            arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("selected_ion"));
            if (arr != null) selectedIons.Add(arr);
        }

        if (spectra.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(spectra);
            SpectrumMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }

        if (scans.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(scans);
            ScanMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }
        if (precursors.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(precursors);
            PrecursorMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }
        if (selectedIons.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(selectedIons);
            SelectedIonMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }
    }

    public override SpectrumDescription Get(ulong index)
    {
        return GetSpectrum(index);
    }
}

public class ChromatogramMetadataReader : MetadataReaderBase<ChromatogramDescription>
{
    public ParquetSharp.Arrow.FileReader FileReader;

    RecordBatch? chromatogramMetadata = null;
    List<ColumnParam> chromatogramMetadataColumns;
    RecordBatch? precursorMetadata = null;
    List<ColumnParam> precursorMetadataColumns;
    RecordBatch? selectedIonMetadata = null;
    List<ColumnParam> selectedIonMetadataColumns;

    public override int Length
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

    public override List<ChromatogramDescription> BulkLoad()
    {
        if (ChromatogramMetadata == null) return [];
        var chromVisitor = new ChromatogramVisitor();
        chromVisitor.Visit(ChromatogramMetadata);
        var descrs = chromVisitor.Values.Select(rec => new ChromatogramDescription(rec, new(), new())).ToList();
        if (PrecursorMetadata != null)
        {
            var visitor = new PrecursorVisitor();
            visitor.Visit(PrecursorMetadata);
            foreach (var rec in visitor.Values)
            {
                descrs[(int)rec.SourceIndex].Precursors.Add(rec);
            }
        }
        if (SelectedIonMetadata != null)
        {
            var visitor = new SelectedIonVisitor();
            visitor.Visit(SelectedIonMetadata);
            foreach (var rec in visitor.Values)
            {
                descrs[(int)rec.SourceIndex].SelectedIons.Add(rec);
            }
        }
        return descrs;
    }

    ChromatogramDescription GetChromatogram(ulong index)
    {
        if (ChromatogramMetadata == null) throw new IndexOutOfRangeException($"{index} out of chromatogram index range");

        var idxArr = (UInt64Array)ChromatogramMetadata.Column(0);
        var mask = Compute.Compute.Equal(idxArr, index);
        var recs = Compute.Compute.Filter(ChromatogramMetadata, mask);
        var visitor = new ChromatogramVisitor();
        visitor.Visit(recs);
        var rec = visitor.Values[0];

        List<PrecursorInfo> precursorInfos = new();
        if (PrecursorMetadata != null)
        {
            idxArr = (UInt64Array)PrecursorMetadata.Column(0);
            mask = Compute.Compute.Equal(idxArr, index);
            recs = Compute.Compute.Filter(PrecursorMetadata, mask);
            var scanVisitor = new PrecursorVisitor();
            scanVisitor.Visit(recs);
            precursorInfos = scanVisitor.Values;
        }
        List<SelectedIonInfo> selectedIons = new();
        if (SelectedIonMetadata != null)
        {
            idxArr = (UInt64Array)SelectedIonMetadata.Column(0);
            mask = Compute.Compute.Equal(idxArr, index);
            recs = Compute.Compute.Filter(SelectedIonMetadata, mask);
            var scanVisitor = new SelectedIonVisitor();
            scanVisitor.Visit(recs);
            selectedIons = scanVisitor.Values;
        }

        return new ChromatogramDescription(rec, precursorInfos, selectedIons);
    }

    public async Task InitializeTables()
    {
        var reader = FileReader.GetRecordBatchReader();
        var builder = new RecordBatch.Builder();
        var ctr = 0;
        List<IArrowArray> chromatograms = [];
        List<IArrowArray> precursors = [];
        List<IArrowArray> selectedIons = [];
        while (true)
        {
            RecordBatch batch = await reader.ReadNextRecordBatchAsync();
            if (batch == null)
            {
                Logger?.LogInformation($"Read {ctr} batches from {this}");
                break;
            }
            Logger?.LogInformation("batch {ctr}, {batch.Length} items", batch, ctr);
            ctr++;
            var arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("chromatogram"));
            if (arr != null) chromatograms.Add(arr);
            arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("precursor"));
            if (arr != null) precursors.Add(arr);
            arr = ArrowCompatibilityVisitor.MakeNetCompatible(batch.Column("selected_ion"));
            if (arr != null) selectedIons.Add(arr);
        }

        if (chromatograms.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(chromatograms);
            ChromatogramMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }
        if (precursors.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(precursors);
            PrecursorMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }
        if (selectedIons.Count > 0)
        {
            StructArray bat = (StructArray)ArrowArrayConcatenator.Concatenate(selectedIons);
            SelectedIonMetadata = new RecordBatch(new Schema(((StructType)bat.Data.DataType).Fields, []), bat.Fields, bat.Length);
        }
    }

    public override ChromatogramDescription Get(ulong index)
    {
        return GetChromatogram(index);
    }
}
