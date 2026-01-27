using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;

namespace MZPeak.Reader.Visitors;

public record HasIonMobility
{
    public double? IonMobility {get; set;} = null;
    public string? IonMobilityTypeCURIE { get; set; } = null;
}

public interface IHasParameters
{
    public List<Param> Parameters {get; set;}

    public Param? FindParam(string accession) => Parameters.First(p => p.AccessionCURIE == accession);
    public bool HasParam(string accession) => FindParam(accession) != null;

}

public interface IHasSourceIndex
{
    public ulong SourceIndex {get; set;}
}

public interface IHasPrecursorIndex
{
    public ulong PrecursorIndex { get; set; }
}

public record SpectrumInfo : IHasParameters
{
    public ulong Index { get; set; }
    public string Id { get; set; }
    public double Time { get; set; }
    public byte MSLevel { get; set; }
    public string? DataProcessingRef { get; set; } = null;
    public int NumberOfAuxiliaryArrays { get; set; }
    public List<double>? MzDeltaModel {get; set; } = null;
    public List<Param> Parameters { get; set; }
    public List<AuxiliaryArray> AuxiliaryArrays { get; set; }

    public bool IsProfile => ((IHasParameters)this).HasParam("MS:1000128");
    public bool IsCentroid => ((IHasParameters)this).HasParam("MS1000127");
    public double? BasePeakMZ => ((IHasParameters)this).FindParam(SpectrumProperties.BasePeakMZ.CURIE())?.AsDouble();
    public double? BasePeakIntensity => ((IHasParameters)this).FindParam(SpectrumProperties.BasePeakIntensity.CURIE())?.AsDouble();

    public SpectrumInfo(ulong index, string id, double time, byte msLevel, string? dataProcessingRef=null, int numberOfAuxiliaryArrays=0, List<double>? mzDeltaModel=null, List<Param>? parameters=null, List<AuxiliaryArray>? auxiliaryArray=null)
    {
        Index= index;
        Id = id;
        Time = time;
        MSLevel = msLevel;
        DataProcessingRef = dataProcessingRef;
        NumberOfAuxiliaryArrays = numberOfAuxiliaryArrays;
        MzDeltaModel = mzDeltaModel;
        Parameters = parameters ?? new();
        AuxiliaryArrays = auxiliaryArray ?? new();
    }

    public override string ToString()
    {
        return "SpectrumInfo\n" + JsonSerializer.Serialize(this, new JsonSerializerOptions() { WriteIndented = true, IndentSize = 2 });
    }
}

record ParamListRecord : IHasParameters
{
    public List<Param> Parameters {get; set;}

    public ParamListRecord()
    {
        Parameters = new();
    }
}

public record ScanWindow
{
    public double LowerBound { get; set; }
    public double UpperBound { get; set; }
    public Unit Unit { get; set; }

    public ScanWindow(double lowerBound, double upperBound, Unit unit)
    {
        LowerBound = lowerBound;
        UpperBound = upperBound;
        Unit = unit;
    }
}

public record ScanInfo : HasIonMobility, IHasSourceIndex, IHasParameters
{
    public ulong SourceIndex { get; set; }
    public uint? InstrumentConfigurationRef { get; set; }
    public List<Param> Parameters { get; set; }
    public List<ScanWindow> ScanWindows { get; set; }

    public ScanInfo(ulong sourceIndex, uint? instrumentConfigurationRef=null, double? ionMobility=null, string? ionMobilityTypeCURIE=null, List<Param>? parameters=null, List<ScanWindow>? scanWindows=null)
    {
        SourceIndex = sourceIndex;
        InstrumentConfigurationRef = instrumentConfigurationRef;
        IonMobility = ionMobility;
        IonMobilityTypeCURIE = ionMobilityTypeCURIE;
        Parameters = parameters ?? new();
        ScanWindows = scanWindows ?? new();
    }

    public override string ToString()
    {
        return "ScanInfo\n" + JsonSerializer.Serialize(this, new JsonSerializerOptions() { WriteIndented = true, IndentSize = 2 });
    }
}

public record PrecursorInfo : IHasSourceIndex, IHasPrecursorIndex
{
    public ulong SourceIndex {get; set; }
    public ulong PrecursorIndex {get; set;}
    public string? PrecursorId { get; set; }
    public List<Param> IsolationWindowParameters { get; set; }
    public List<Param> ActivationParameters { get; set; }

    public PrecursorInfo(ulong sourceIndex, ulong precursorIndex, string? precursorId=null, List<Param>? isolationWindowParameters=null, List<Param>? activationParameters=null)
    {
        SourceIndex = sourceIndex;
        PrecursorIndex = precursorIndex;
        PrecursorId = precursorId;
        IsolationWindowParameters = isolationWindowParameters ?? new();
        ActivationParameters = activationParameters ?? new();
    }

    public override string ToString()
    {
        return "PrecursorInfo\n" + JsonSerializer.Serialize(this, new JsonSerializerOptions() { WriteIndented = true, IndentSize = 2 });
    }
}

public record SelectedIonInfo: HasIonMobility, IHasSourceIndex, IHasParameters, IHasPrecursorIndex
{
    public ulong SourceIndex { get; set; }
    public ulong PrecursorIndex { get; set; }
    public List<Param> Parameters { get; set; }

    public SelectedIonInfo(ulong sourceIndex, ulong precursorIndex, double? ionMobility=null, string? ionMobilityTypeCURIE=null, List<Param>? parameters=null)
    {
        SourceIndex = sourceIndex;
        PrecursorIndex = precursorIndex;
        IonMobility = ionMobility;
        IonMobilityTypeCURIE = ionMobilityTypeCURIE;
        Parameters = parameters ?? new();
    }

    public override string ToString()
    {
        return "SelectedIonInfo\n" + JsonSerializer.Serialize(this, new JsonSerializerOptions() { WriteIndented = true, IndentSize = 2 });
    }
}

public record SpectrumDescription
{
    SpectrumInfo SpectrumInfo;
    public List<ScanInfo> Scans;
    public List<PrecursorInfo> Precursors;
    public List<SelectedIonInfo> SelectedIons;

    public string Id => SpectrumInfo.Id;
    public ulong Index => SpectrumInfo.Index;
    public byte MSLevel => SpectrumInfo.MSLevel;
    public double Time => SpectrumInfo.Time;
    public List<Param> Parameters => SpectrumInfo.Parameters;
    public List<double>? MzDeltaModel => SpectrumInfo.MzDeltaModel;
    public string? DataProcessingRef => SpectrumInfo.DataProcessingRef;

    public SpectrumDescription(SpectrumInfo spectrumInfo, List<ScanInfo> scans, List<PrecursorInfo> precursors, List<SelectedIonInfo> selectedIons)
    {
        SpectrumInfo = spectrumInfo;
        Scans = scans;
        Precursors = precursors;
        SelectedIons = selectedIons;
    }
}

public interface IVisitorAssemblyWithOffsets<T>
{
    List<int> Offsets { get; }
    List<T> Values { get; }
}

public interface IHasIonMobilityVisitor<T>: IVisitorAssemblyWithOffsets<T> where T: HasIonMobility
{

    public void VisitIonMobilityType(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.String)
        {
            StringArray arr = (StringArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetString(i);
                Values[j].IonMobilityTypeCURIE = chunk;
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.LargeString)
        {
            LargeStringArray arr = (LargeStringArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetString(i);
                Values[j].IonMobilityTypeCURIE = chunk;
            }
        }
        else throw new NotImplementedException();
    }

    public void VisitIonMobilityValue(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.Float)
        {
            FloatArray arr = (FloatArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                var chunk = arr.GetValue(i);
                if (chunk == null) continue;
                Values[j].IonMobility = chunk;
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.Double)
        {
            DoubleArray arr = (DoubleArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                var chunk = arr.GetValue(i);
                if (chunk == null) continue;
                Values[j].IonMobility = chunk;
            }
        }
    }
}

public interface IHasParametersVisitor<T>: IVisitorAssemblyWithOffsets<T> where T: IHasParameters
{
    public IEnumerable<(int, long?)> VisitInteger<U>(PrimitiveArray<U> array) where U: struct, INumber<U>
    {
        for (int j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            var value = array.GetValue(i);
            if (value == null)
                yield return (i, null);
            else
            {
                var v = (U)value;
                yield return (i, long.CreateChecked(v));
            }
        }
    }

    public IEnumerable<(int, long?)> VisitInteger(IArrowArray array)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Int8:
            {
                return VisitInteger((Int8Array)array);
            }
            case ArrowTypeId.Int16:
            {
                return VisitInteger((Int16Array)array);
            }
            case ArrowTypeId.Int32:
            {
                return VisitInteger((Int32Array)array);
            }
            case ArrowTypeId.Int64:
            {
                return VisitInteger((Int64Array)array);
            }
            case ArrowTypeId.UInt8:
            {
                return VisitInteger((UInt8Array)array);
            }
            case ArrowTypeId.UInt16:
            {
                return VisitInteger((UInt16Array)array);
            }
            case ArrowTypeId.UInt32:
            {
                return VisitInteger((UInt32Array)array);
            }
            case ArrowTypeId.UInt64:
            {
                return VisitInteger((UInt64Array)array);
            }
            default: throw new InvalidCastException($"Could not convert {array.Data.DataType.Name} to an integer");
        }
    }

    public IEnumerable<(int, double?)> VisitFloat<U>(PrimitiveArray<U> array) where U : struct, INumber<U>
    {
        for (int j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            var value = array.GetValue(i);
            if (value == null)
                yield return (i, null);
            else
            {
                var v = (U)value;
                yield return (i, double.CreateChecked(v));
            }
        }
    }

    public IEnumerable<(int, string?)> VisitString(StringArray array)
    {
        for (int j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            if (array.IsNull(i))
                yield return (i, null);
            else
            {
                var v = array.GetString(i);
                yield return (i, v);
            }
        }
    }

    public IEnumerable<(int, string?)> VisitString(LargeStringArray array)
    {
        for (int j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            if (array.IsNull(i))
                yield return (i, null);
            else
            {
                var v = array.GetString(i);
                yield return (i, v);
            }
        }
    }

    void VisitParameters(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.List)
        {
            ListArray arr = (ListArray)array;
            var paramVisitor = new ParamVisitor();
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                var chunk = arr.GetSlicedValues(i);
                paramVisitor.Visit(chunk);
                Values[j].Parameters.AddRange(paramVisitor.Params);
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.LargeList)
        {
            LargeListArray arr = (LargeListArray)array;
            var paramVisitor = new ParamVisitor();
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                var chunk = arr.GetSlicedValues(i);
                paramVisitor.Visit(chunk);
                Values[j].Parameters.AddRange(paramVisitor.Params);
            }
        }
        else throw new NotImplementedException();
    }

    void VisitAsParameter(Field field, IArrowArray array)
    {
        var param = ColumnParam.FromFieldIndex(field, 0);
        if (param.IsUnitOnly) throw new NotImplementedException();
        switch (array.Data.DataType.TypeId) {
            case ArrowTypeId.Int8:
                {
                    foreach ((var i, var val) in VisitInteger((Int8Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.Int16:
                {
                    foreach((var i, var val) in VisitInteger((Int16Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    foreach ((var i, var val) in VisitInteger((Int32Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    foreach ((var i, var val) in VisitInteger((Int64Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.UInt8:
                {
                    foreach ((var i, var val) in VisitInteger((UInt8Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.UInt16:
                {
                    foreach ((var i, var val) in VisitInteger((UInt16Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.UInt32:
                {
                    foreach ((var i, var val) in VisitInteger((UInt32Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.UInt64:
                {
                    foreach ((var i, var val) in VisitInteger((UInt64Array)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.Float:
                {
                    foreach ((var i, var val) in VisitFloat((FloatArray)array))
                    {
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    }
                    break;
                }
            case ArrowTypeId.Double:
                {
                    foreach ((var i, var val) in VisitFloat((DoubleArray)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    BooleanArray arr = (BooleanArray)array;
                    for(int j = 0; j < Offsets.Count; j++)
                    {
                        var i = Offsets[j];
                        var value = arr.GetValue(i);
                        Values[j].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: value, param.UnitCURIE));
                    }
                    break;
                }
            case ArrowTypeId.String:
                {
                    foreach ((var i, var val) in VisitString((StringArray)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            case ArrowTypeId.LargeString:
                {
                    foreach ((var i, var val) in VisitString((LargeStringArray)array))
                        Values[i].Parameters.Add(new Param(param.Name, accession: param.CURIE, rawValue: val, param.UnitCURIE));
                    break;
                }
            default: throw new NotImplementedException($"{array.Data.DataType.Name} from {field.Name}");
        }
    }
}

public interface IHasSourceIndexVisitor<T>: IVisitorAssemblyWithOffsets<T> where T: IHasSourceIndex
{
    public T CreateFromIndex(ulong index);

    public void VisitSourceIndex(IArrowArray array)
    {
        UInt64Array arr = (UInt64Array)array;
        for (int i = 0; i < arr.Length; i++)
        {
            var idx = arr.GetValue(i);
            if (idx == null) continue;
            Offsets.Add(i);
            Values.Add(CreateFromIndex((ulong)idx));
        }
    }
}

public interface IHasPrecursorIndexVisitor<T> : IVisitorAssemblyWithOffsets<T> where T: IHasPrecursorIndex
{
    public void VisitPrecursorIndex(IArrowArray array)
    {
        UInt64Array arr = (UInt64Array)array;
        for (int j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            var chunk = arr.GetValue(i);
            if (chunk == null) continue;
            Values[j].PrecursorIndex = (ulong)chunk;
        }
    }
}

class GenericParamStructVisitor : IVisitorAssemblyWithOffsets<ParamListRecord>, IHasParametersVisitor<ParamListRecord>, IArrowArrayVisitor<StructArray>, IArrowArrayVisitor<ListArray>, IArrowArrayVisitor<LargeListArray>
{
    public List<ParamListRecord> Values {get; set;}
    public List<int> Offsets {get; set;}

    public GenericParamStructVisitor(List<int> offsets)
    {
        Values = new();
        Offsets = offsets;
    }

    void Initialize(IArrowArray array)
    {
        for (var i = 0; i < Offsets.Count; i++)
        {
            // Offsets.Add(i);
            Values.Add(new());
        }
    }

    public void Visit(StructArray array)
    {
        Initialize(array);
        var dtype = (StructType)array.Data.DataType;
        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "parameters") ((IHasParametersVisitor<ParamListRecord>)this).VisitParameters(arr);
            else ((IHasParametersVisitor<ParamListRecord>)this).VisitAsParameter(f, arr);
        }
    }

    public void Visit(ListArray array)
    {
        Initialize(array);
        for(var j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            var chunk = array.GetSlicedValues(i);
            var visitor = new ParamVisitor();
            visitor.Visit(chunk);
            Values[j].Parameters = visitor.Params;
        }

    }

    public void Visit(LargeListArray array)
    {
        Initialize(array);
        for (var j = 0; j < Offsets.Count; j++)
        {
            var i = Offsets[j];
            var chunk = array.GetSlicedValues(i);
            var visitor = new ParamVisitor();
            visitor.Visit(chunk);
            Values[j].Parameters = visitor.Params;
        }
    }

    public void Visit(IArrowArray array)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Struct:
                {
                    Visit((StructArray)array);
                    break;
                }
            case ArrowTypeId.List:
                {
                    Visit((ListArray)array);
                    break;
                }
            case ArrowTypeId.LargeList:
                {
                    Visit((LargeListArray)array);
                    break;
                }
            default: throw new InvalidDataException($"Invalid data type {array.Data.DataType.Name} not valid for parameter collections");
        }
    }
}

public class ScanVisitor : IVisitorAssemblyWithOffsets<ScanInfo>, IHasIonMobilityVisitor<ScanInfo>, IHasParametersVisitor<ScanInfo>, IArrowArrayVisitor<StructArray>, IHasSourceIndexVisitor<ScanInfo>, IArrowArrayVisitor<RecordBatch>
{
    public List<ScanInfo> Values {get; set;}
    public List<int> Offsets {get; set;}

    public ScanVisitor()
    {
        Values = new();
        Offsets = new();
    }

    void VisitInstrumentConfigurationRef(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.UInt32)
        {
            UInt32Array arr = (UInt32Array)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                var chunk = arr.GetValue(i);
                Values[j].InstrumentConfigurationRef = chunk;
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.Int32)
        {
            UInt32Array arr = (UInt32Array)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                var chunk = arr.GetValue(i);
                Values[j].InstrumentConfigurationRef = chunk;
            }
        }
        else throw new NotImplementedException(array.Data.DataType.Name);
    }

    public void Visit(StructArray array)
    {
        Values = new();
        Offsets.Clear();

        var dtype = (StructType)array.Data.DataType;

        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "source_index")
            {
                ((IHasSourceIndexVisitor<ScanInfo>)this).VisitSourceIndex(arr);
                break;
            }
        }

        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "instrument_configuration_ref") VisitInstrumentConfigurationRef(arr);
            else if (f.Name == "ion_mobility_value") ((IHasIonMobilityVisitor<ScanInfo>)this).VisitIonMobilityValue(arr);
            else if (f.Name == "ion_mobility_type") ((IHasIonMobilityVisitor<ScanInfo>)this).VisitIonMobilityType(arr);
            else if (f.Name == "parameters") ((IHasParametersVisitor<ScanInfo>)this).VisitParameters(arr);
            else if (f.Name == "scan_windows")
            {
                //TODO: sub-visitor
            }
            else if (f.Name == "source_index") {}
            else {
                ((IHasParametersVisitor<ScanInfo>)this).VisitAsParameter(f, arr);
            }
        }
    }

    public void Visit(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.Struct) Visit((StructArray)array);
        else throw new InvalidDataException();
    }

    public ScanInfo CreateFromIndex(ulong index)
    {
        return new ScanInfo(index, null, null, null, null);
    }

    public void Visit(RecordBatch array)
    {
        var dtype = new StructType(array.Schema.FieldsList);
        Visit(new StructArray(dtype, array.Length, array.Arrays, default));
    }
}

public class SelectedIonVisitor : IVisitorAssemblyWithOffsets<SelectedIonInfo>, IHasIonMobilityVisitor<SelectedIonInfo>, IHasParametersVisitor<SelectedIonInfo>, IArrowArrayVisitor<StructArray>, IArrowArrayVisitor<RecordBatch>, IHasSourceIndexVisitor<SelectedIonInfo>, IHasPrecursorIndexVisitor<SelectedIonInfo>
{

    public List<SelectedIonInfo> Values {get; set;}
    public List<int> Offsets {get; set;}


    public SelectedIonVisitor()
    {
        Values = new();
        Offsets = new();
    }

    public SelectedIonInfo CreateFromIndex(ulong index)
    {
        return new SelectedIonInfo(index, 0, null, null, null);
    }

    public void Visit(StructArray array)
    {
        Values = new();
        Offsets.Clear();

        var dtype = (StructType)array.Data.DataType;

        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "source_index")
            {
                ((IHasSourceIndexVisitor<SelectedIonInfo>)this).VisitSourceIndex(arr);
                break;
            }
        }
        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "precursor_index") ((IHasPrecursorIndexVisitor<SelectedIonInfo>)this).VisitPrecursorIndex(arr);
            else if (f.Name == "source_index") {}
            else if (f.Name == "ion_mobility_value") ((IHasIonMobilityVisitor<SelectedIonInfo>)this).VisitIonMobilityValue(arr);
            else if (f.Name == "ion_mobility_type") ((IHasIonMobilityVisitor<SelectedIonInfo>)this).VisitIonMobilityType(arr);
            else if (f.Name == "parameters") ((IHasParametersVisitor<SelectedIonInfo>)this).VisitParameters(arr);
            else
            {
                ((IHasParametersVisitor<SelectedIonInfo>)this).VisitAsParameter(f, arr);
            }
        }
    }

    public void Visit(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.Struct) Visit((StructArray)array);
        else throw new InvalidDataException();
    }

    public void Visit(RecordBatch array)
    {
        var dtype = new StructType(array.Schema.FieldsList);
        Visit(new StructArray(dtype, array.Length, array.Arrays, default));
    }
}

public class PrecursorVisitor : IVisitorAssemblyWithOffsets<PrecursorInfo>, IHasSourceIndexVisitor<PrecursorInfo>, IHasPrecursorIndexVisitor<PrecursorInfo>, IArrowArrayVisitor<StructArray>, IArrowArrayVisitor<RecordBatch>
{
    public List<PrecursorInfo> Values { get; set; }
    public List<int> Offsets { get; set; }

    public PrecursorVisitor()
    {
        Values = new();
        Offsets = new();
    }

    public void VisitPrecursorId(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.String)
        {
            StringArray arr = (StringArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetString(i);
                Values[j].PrecursorId = chunk;
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.LargeString)
        {
            LargeStringArray arr = (LargeStringArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetString(i);
                Values[j].PrecursorId = chunk;
            }
        }
        else throw new NotImplementedException();
    }

    public void Visit(StructArray array)
    {
        Values = new();
        Offsets.Clear();

        var dtype = (StructType)array.Data.DataType;

        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "source_index")
            {
                ((IHasSourceIndexVisitor<PrecursorInfo>)this).VisitSourceIndex(arr);
                break;
            }
        }
        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "precursor_index") ((IHasPrecursorIndexVisitor<PrecursorInfo>)this).VisitPrecursorIndex(arr);
            else if (f.Name == "precursor_id") VisitPrecursorId(arr);
            else if (f.Name == "activation") VisitActivationParameters(arr);
            else if (f.Name == "isolation_window") VisitIsolationWindowParameters(arr);
            else if (f.Name == "source_index") {}
            else {}
        }
    }

    public void Visit(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.Struct) Visit((StructArray)array);
        else throw new InvalidDataException();
    }

    void VisitActivationParameters(IArrowArray array)
    {
        var visitor = new GenericParamStructVisitor(Offsets);
        visitor.Visit(array);
        for (var i = 0; i < Values.Count; i++)
        {
            Values[i].ActivationParameters = visitor.Values[i].Parameters;
        }
    }

    void VisitIsolationWindowParameters(IArrowArray array)
    {
        var visitor = new GenericParamStructVisitor(Offsets);
        visitor.Visit(array);
        for(var i = 0; i < Values.Count; i++)
        {
            Values[i].IsolationWindowParameters = visitor.Values[i].Parameters;
        }
    }

    public PrecursorInfo CreateFromIndex(ulong index)
    {
        return new PrecursorInfo(index, 0, null, null, null);
    }

    public void Visit(RecordBatch array)
    {
        var dtype = new StructType(array.Schema.FieldsList);
        Visit(new StructArray(dtype, array.Length, array.Arrays, default));
    }
}

public class SpectrumVisitor : IVisitorAssemblyWithOffsets<SpectrumInfo>, IHasParametersVisitor<SpectrumInfo>, IArrowArrayVisitor<StructArray>, IArrowArrayVisitor<RecordBatch>
{
    public List<SpectrumInfo> Values { get; set; }
    public List<int> Offsets { get; set; }

    public SpectrumVisitor()
    {
        Values = new();
        Offsets = new();
    }

    void VisitId(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.String)
        {
            StringArray arr = (StringArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetString(i);
                Values[j].Id = chunk;
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.LargeString)
        {
            LargeStringArray arr = (LargeStringArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetString(i);
                Values[j].Id = chunk;
            }
        }
        else throw new NotImplementedException();
    }

    public void Visit(RecordBatch array)
    {
        var dtype = new StructType(array.Schema.FieldsList);
        Visit(new StructArray(dtype, array.Length, array.Arrays, default));
    }

    public void Visit(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.Struct) Visit((StructArray)array);
        else throw new InvalidDataException();
    }

    public void VisitTime(IArrowArray array)
    {
        IHasParametersVisitor<SpectrumInfo> self = this;
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Float:
                {
                    foreach((var i, var val) in self.VisitFloat((FloatArray)array))
                    {
                        Values[i].Time = val ?? default;
                    }
                    break;
                }
            case ArrowTypeId.Double:
                {
                    foreach ((var i, var val) in self.VisitFloat((DoubleArray)array))
                    {
                        Values[i].Time = val ?? default;
                    }
                    break;
                }
            default: throw new NotImplementedException();
        }
    }

    public void VisitMSLevel(IArrowArray array)
    {
        var self = (IHasParametersVisitor<SpectrumInfo>)this;
        foreach ((var i, var val) in self.VisitInteger(array))
            Values[i].MSLevel = (byte)(val ?? 0);
    }

    public void VisitNumberOfAuxiliaryArrays(IArrowArray array)
    {
        var self = (IHasParametersVisitor<SpectrumInfo>)this;
        foreach((var i, var val) in self.VisitInteger(array))
            Values[i].NumberOfAuxiliaryArrays = (int)(val ?? 0);
    }

    public void VisitMzDeltaModel(IArrowArray array)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.List:
                {
                    var arr = (ListArray)array;
                    for(var j = 0; j < Offsets.Count; j++)
                    {
                        var i = Offsets[j];
                        if (!arr.IsNull(i)) {
                            var p = (DoubleArray)arr.GetSlicedValues(i);
                            Values[j].MzDeltaModel = p.Where(v => v != null).Select(v => v == null ? default : (double)v).ToList();
                        }
                    }
                    break;
                }
            case ArrowTypeId.LargeList:
                {
                    var arr = (LargeListArray)array;
                    for (var j = 0; j < Offsets.Count; j++)
                    {
                        var i = Offsets[j];
                        if (!arr.IsNull(i))
                        {
                            var p = (DoubleArray)arr.GetSlicedValues(i);
                            Values[j].MzDeltaModel = p.Where(v => v != null).Select(v => v == null ? default : (double)v).ToList();
                        }
                    }
                    break;
                }
            case ArrowTypeId.Double:
                {
                    var arr = (DoubleArray)array;
                    for (var j = 0; j < Offsets.Count; j++)
                    {
                        var i = Offsets[j];
                        var p = arr.GetValue(i);
                        if (p != null)
                            Values[j].MzDeltaModel = new() { (double)p };
                    }
                    break;
                }
            default: throw new NotImplementedException(array.Data.DataType.Name);
        }
    }

    public void VisitDataProcessingRef(IArrowArray array) { }

    public void Visit(StructArray array)
    {
        Values = new();
        Offsets.Clear();

        var dtype = (StructType)array.Data.DataType;

        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "index")
            {
                VisitIndex(arr);
                break;
            }
        }

        var self = (IHasParametersVisitor<SpectrumInfo>)this;

        foreach (var (f, arr) in dtype.Fields.Zip(array.Fields))
        {
            if (f.Name == "id") VisitId(arr);
            else if (f.Name == "index") {}
            else if (f.Name == "time") VisitTime(arr);
            else if (f.Name == "MS_1000511_ms_level") VisitMSLevel(arr);
            else if (f.Name == "data_processing_ref") VisitDataProcessingRef(arr);
            else if (f.Name == "mz_delta_model") VisitMzDeltaModel(arr);
            else if (f.Name == "number_of_auxiliary_arrays") VisitNumberOfAuxiliaryArrays(arr);
            else if (f.Name == "auxiliary_arrays") VisitAuxiliarArrays(arr);
            else if (f.Name == "parameters") self.VisitParameters(arr);
            else self.VisitAsParameter(f, arr);
        }
    }

    void VisitAuxiliarArrays(IArrowArray array)
    {
        if (array.Data.DataType.TypeId == ArrowTypeId.List)
        {
            ListArray arr = (ListArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetSlicedValues(i);
                var visitor = new AuxiliaryArrayVisitor();
                visitor.Visit(chunk);
                Values[j].AuxiliaryArrays.AddRange(visitor.Values);
            }
        }
        else if (array.Data.DataType.TypeId == ArrowTypeId.LargeList)
        {
            LargeListArray arr = (LargeListArray)array;
            for (int j = 0; j < Offsets.Count; j++)
            {
                var i = Offsets[j];
                if (arr.IsNull(i)) continue;
                var chunk = arr.GetSlicedValues(i);
                var visitor = new AuxiliaryArrayVisitor();
                visitor.Visit(chunk);
                Values[j].AuxiliaryArrays.AddRange(visitor.Values);
            }
        }
        else throw new NotImplementedException();
    }

    public SpectrumInfo CreateFromIndex(ulong index)
    {
        return new SpectrumInfo(index, "", 0.0, 0);
    }

    public void VisitIndex(IArrowArray array)
    {
        UInt64Array arr = (UInt64Array)array;
        for (int i = 0; i < arr.Length; i++)
        {
            var idx = arr.GetValue(i);
            if (idx == null) continue;
            Offsets.Add(i);
            Values.Add(CreateFromIndex((ulong)idx));
        }
    }
}

