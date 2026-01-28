using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;

namespace MZPeak.Writer.Visitors;

public class IsolationWindowBuilder : ParamVisitorCollection, IArrowBuilder<List<Param>>
{
    public int Length => ParamVisitors[0].Length;

    public IsolationWindowBuilder() : base(new ()
    {
        new CustomBuilderFromParam("MS:1000827", "isolation window target m/z", new DoubleType(), "MS:1000040"),
        new CustomBuilderFromParam("MS:1000828", "isolation window lower offset", new DoubleType(), "MS:1000040"),
        new CustomBuilderFromParam("MS:1000829", "isolation window upper offset", new DoubleType(), "MS:1000040"),
    }){}

    public void Append(List<Param> value)
    {
        VisitParameters(value);
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>();
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.ArrowType());
        }
        fields.AddRange(ParamList.ArrowType());
        return new(){new Field("isolation_window", new StructType(fields), true)};
    }

    public List<IArrowArray> Build()
    {
        var fields = new List<IArrowArray>();
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.Build());
        }
        fields.AddRange(ParamList.Build());
        return new(){new StructArray(ArrowType()[0].DataType, fields[0].Length, fields, default)};
    }

    public void Clear()
    {
        foreach (var vis in ParamVisitors)
        {
            vis.Clear();
        }
        ParamList.Clear();
    }
}

public class ActivationBuilder : ParamVisitorCollection, IArrowBuilder<List<Param>>
{
    public int Length => ParamVisitors[0].Length;

    public ActivationBuilder() : base(new()
    {
        new CustomBuilderFromParam("MS:1000045", "collision energy", new DoubleType(), "UO:0000266"),
        new CustomBuilderFromParam("MS:1000044", "dissociation method", new StringType()),
    })
    { }

    public void Append(List<Param> value)
    {
        Visited.Clear();
        VisitParameters(value);
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>();
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.ArrowType());
        }
        fields.AddRange(ParamList.ArrowType());
        return new() { new Field("activation", new StructType(fields), true) };
    }

    public List<IArrowArray> Build()
    {
        var fields = new List<IArrowArray>();
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.Build());
        }
        fields.AddRange(ParamList.Build());
        return new() { new StructArray(ArrowType()[0].DataType, fields[0].Length, fields, default) };
    }

    public void Clear()
    {
        foreach (var vis in ParamVisitors)
        {
            vis.Clear();
        }
        ParamList.Clear();
    }
}

public class PrecursorBuilder : IArrowBuilder<(ulong, ulong, string?, List<Param>, List<Param>)>
{
    UInt64Array.Builder SourceIndex;
    UInt64Array.Builder PrecursorIndex;
    StringArray.Builder PrecursorId;
    IsolationWindowBuilder IsolationWindow;
    ActivationBuilder Activation;

    public int Length => SourceIndex.Length;

    public PrecursorBuilder()
    {
        SourceIndex = new();
        PrecursorIndex = new();
        PrecursorId = new();
        IsolationWindow = new();
        Activation = new();
    }

    public void Append((ulong, ulong, string?, List<Param>, List<Param>) value)
    {
        Append(value.Item1, value.Item2, value.Item3, value.Item4, value.Item5);
    }

    public void Append(ulong sourceIndex, ulong precursorIndex, string? precursorId, List<Param> isolationWindowParams, List<Param> activationParams)
    {
        SourceIndex.Append(sourceIndex);
        PrecursorIndex.Append(precursorIndex);
        if (precursorId != null) PrecursorId.Append(precursorId);
        else PrecursorId.AppendNull();
        IsolationWindow.Append(isolationWindowParams);
        Activation.Append(activationParams);
    }

    public void AppendNull()
    {
        SourceIndex.AppendNull();
        PrecursorIndex.AppendNull();
        PrecursorId.AppendNull();
        IsolationWindow.AppendNull();
        Activation.AppendNull();
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>()
        {
            new Field("source_index", new UInt64Type(), true),
            new Field("precursor_index", new UInt64Type(), true),
            new Field("precursor_id", new StringType(), true)
        };
        fields.AddRange(IsolationWindow.ArrowType());
        fields.AddRange(Activation.ArrowType());
        return new() { new Field("precursor", new StructType(fields), true) };
    }

    public List<IArrowArray> Build()
    {
        List<IArrowArray> fields =
        [
            SourceIndex.Build(),
            PrecursorIndex.Build(),
            PrecursorId.Build(),
            .. IsolationWindow.Build(),
            .. Activation.Build(),
        ];
        var size = SourceIndex.Length;
        Clear();
        return new() { new StructArray(ArrowType()[0].DataType, size, fields, default) };
    }

    public void Clear()
    {
        SourceIndex.Clear();
        PrecursorIndex.Clear();
        PrecursorId.Clear();
        IsolationWindow.Clear();
        Activation.Clear();
    }
}

public class SpectrumBuilder : ParamVisitorCollection, IArrowBuilder<(ulong, string, double, string?, List<double>?, List<Param>, List<AuxiliaryArray>)>
{
    UInt64Array.Builder Index;
    StringArray.Builder Id;
    DoubleArray.Builder Time;
    StringArray.Builder DataProcessingRef;
    ListArray.Builder MzDeltaModel;
    Int32Array.Builder NumberOfAuxiliaryArrays;
    AuxiliaryArrayListBuilder AuxiliaryArrays;

    public int Length => Index.Length;

    public SpectrumBuilder() : base(new()
    {
        // Required CV terms
        new CustomBuilderFromParam("MS:1000511", "ms level", new Int64Type()),
        new CustomBuilderFromParam("MS:1000525", "spectrum representation", new StringType()),
        new CustomBuilderFromParam("MS:1000465", "scan polarity", new Int64Type()),
        new CustomBuilderFromParam("MS:1000559", "spectrum type", new StringType()),
        // Optional spectrum properties (commonly present)
        new CustomBuilderFromParam("MS:1003060", "number of data points", new Int64Type()),
        new CustomBuilderFromParam("MS:1000504", "base peak m/z", new DoubleType(), "MS:1000040"),
        new CustomBuilderFromParam("MS:1000505", "base peak intensity", new DoubleType(), "MS:1000131"),
        new CustomBuilderFromParam("MS:1000285", "total ion current", new DoubleType(), "MS:1000131"),
        new CustomBuilderFromParam("MS:1000528", "lowest observed m/z", new DoubleType(), "MS:1000040"),
        new CustomBuilderFromParam("MS:1000527", "highest observed m/z", new DoubleType(), "MS:1000040"),
    })
    {
        Index = new();
        Id = new();
        Time = new();
        DataProcessingRef = new();
        NumberOfAuxiliaryArrays = new();
        MzDeltaModel = new ListArray.Builder(new DoubleType());
        AuxiliaryArrays = new();
    }

    public void Append((ulong, string, double, string?, List<double>?, List<Param>, List<AuxiliaryArray>) value)
    {
        Append(value.Item1, value.Item2, value.Item3, value.Item4, value.Item5, value.Item6, value.Item7);
    }

    public void Append(ulong index, string id, double time, string? dataProcessingRef, List<double>? mzDeltaModel, List<Param> parameters, List<AuxiliaryArray>? auxiliaryArrays=null)
    {
        Index.Append(index);
        Id.Append(id);
        Time.Append(time);
        if (dataProcessingRef != null) DataProcessingRef.Append(dataProcessingRef);
        else DataProcessingRef.AppendNull();
        NumberOfAuxiliaryArrays.Append(auxiliaryArrays?.Count ?? 0);
        AuxiliaryArrays.Append(auxiliaryArrays ?? []);
        if (mzDeltaModel != null)
        {
            var valueBuilder = (DoubleArray.Builder)MzDeltaModel.ValueBuilder;
            foreach (var v in mzDeltaModel)
            {
                valueBuilder.Append(v);
            }
            MzDeltaModel.Append();
        }
        else
        {
            MzDeltaModel.AppendNull();
        }
        Visited.Clear();
        VisitParameters(parameters);
    }

    public override void AppendNull()
    {
        Index.AppendNull();
        Id.AppendNull();
        Time.AppendNull();
        DataProcessingRef.AppendNull();
        NumberOfAuxiliaryArrays.AppendNull();
        MzDeltaModel.AppendNull();
        AuxiliaryArrays.AppendNull();
        base.AppendNull();
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>()
        {
            new Field("index", new UInt64Type(), true),
            new Field("id", new StringType(), true),
            new Field("time", new DoubleType(), true),
            new Field("data_processing_ref", new StringType(), true),
            new Field("number_of_auxiliary_arrays", new Int32Type(), true),
            new Field("mz_delta_model", new ListType(new DoubleType()), true),
            new Field("auxiliary_arrays", AuxiliaryArrays.ArrowType()[0].DataType, true)
        };
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.ArrowType());
        }
        fields.AddRange(ParamList.ArrowType());
        return new() { new Field("spectrum", new StructType(fields), true) };
    }

    public List<IArrowArray> Build()
    {
        List<IArrowArray> fields = new()
        {
            Index.Build(),
            Id.Build(),
            Time.Build(),
            DataProcessingRef.Build(),
            NumberOfAuxiliaryArrays.Build(),
            MzDeltaModel.Build(),
            AuxiliaryArrays.Build()[0]
        };
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.Build());
        }
        fields.AddRange(ParamList.Build());
        var size = Index.Length;

        return new() { new StructArray(ArrowType()[0].DataType, size, fields, default) };
    }

    public void Clear()
    {
        Index.Clear();
        Id.Clear();
        Time.Clear();
        DataProcessingRef.Clear();
        NumberOfAuxiliaryArrays.Clear();
        MzDeltaModel.Clear();
        AuxiliaryArrays.Clear();
        foreach (var vis in ParamVisitors)
        {
            vis.Clear();
        }
        ParamList.Clear();
    }
}

public class ScanBuilder : ParamVisitorCollection, IArrowBuilder<(ulong, uint?, double?, string?, List<Param>)>
{
    UInt64Array.Builder SourceIndex;
    UInt32Array.Builder InstrumentConfigurationRef;
    DoubleArray.Builder IonMobility;
    StringArray.Builder IonMobilityType;

    public int Length => SourceIndex.Length;

    public ScanBuilder() : base(new()
    {
        new CustomBuilderFromParam("MS:1000016", "scan start time", new DoubleType(), "UO:0000031"),
        new CustomBuilderFromParam("MS:1000512", "filter string", new StringType()),
        new CustomBuilderFromParam("MS:1000616", "preset scan configuration", new Int64Type()),
        new CustomBuilderFromParam("MS:1000927", "ion injection time", new DoubleType(), "UO:0000028"),
    })
    {
        SourceIndex = new();
        InstrumentConfigurationRef = new();
        IonMobility = new();
        IonMobilityType = new();
    }

    public void Append((ulong, uint?, double?, string?, List<Param>) value)
    {
        Append(value.Item1, value.Item2, value.Item3, value.Item4, value.Item5);
    }

    public void Append(ulong sourceIndex, uint? instrumentConfigurationRef, double? ionMobility, string? ionMobilityType, List<Param> parameters)
    {
        SourceIndex.Append(sourceIndex);
        if (instrumentConfigurationRef != null) InstrumentConfigurationRef.Append(instrumentConfigurationRef);
        else InstrumentConfigurationRef.AppendNull();
        if (ionMobility.HasValue) IonMobility.Append(ionMobility.Value);
        else IonMobility.AppendNull();
        if (ionMobilityType != null) IonMobilityType.Append(ionMobilityType);
        else IonMobilityType.AppendNull();
        Visited.Clear();
        VisitParameters(parameters);
    }

    public override void AppendNull()
    {
        SourceIndex.AppendNull();
        InstrumentConfigurationRef.AppendNull();
        IonMobility.AppendNull();
        IonMobilityType.AppendNull();
        base.AppendNull();
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>()
        {
            new Field("source_index", new UInt64Type(), true),
            new Field("instrument_configuration_ref", new UInt32Type(), true),
            new Field("ion_mobility", new DoubleType(), true),
            new Field("ion_mobility_type", new StringType(), true)
        };
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.ArrowType());
        }
        fields.AddRange(ParamList.ArrowType());
        return new() { new Field("scan", new StructType(fields), true) };
    }

    public List<IArrowArray> Build()
    {
        List<IArrowArray> fields = new() { SourceIndex.Build(), InstrumentConfigurationRef.Build(), IonMobility.Build(), IonMobilityType.Build() };
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.Build());
        }
        fields.AddRange(ParamList.Build());
        var size = SourceIndex.Length;
        return new() { new StructArray(ArrowType()[0].DataType, size, fields, default) };
    }

    public void Clear()
    {
        SourceIndex.Clear();
        InstrumentConfigurationRef.Clear();
        IonMobility.Clear();
        IonMobilityType.Clear();
        foreach (var vis in ParamVisitors)
        {
            vis.Clear();
        }
        ParamList.Clear();
    }
}

public class SelectedIonBuilder : ParamVisitorCollection, IArrowBuilder<(ulong, ulong, List<Param>)>
{
    UInt64Array.Builder SourceIndex;
    UInt64Array.Builder PrecursorIndex;
    DoubleArray.Builder IonMobility;
    StringArray.Builder IonMobilityType;

    public int Length => SourceIndex.Length;

    public SelectedIonBuilder() : base(new()
        {
            new CustomBuilderFromParam("MS:1000744", "selected ion m/z", new DoubleType(), "MS:1000040"),
            new CustomBuilderFromParam("MS:1000042", "peak intensity", new DoubleType(), "MS:1000131"),
            new CustomBuilderFromParam("MS:1000041", "charge state", new Int64Type())
        })
    {
        SourceIndex = new();
        PrecursorIndex = new();
        IonMobility = new();
        IonMobilityType = new();
    }

    public void Append((ulong, ulong, List<Param>) value)
    {
        Append(value.Item1, value.Item2, null, null, value.Item3);
    }

    public void Append(ulong sourceIndex, ulong precursorIndex, double? ionMobility, string? ionMobilityType, List<Param> parameters)
    {
        SourceIndex.Append(sourceIndex);
        PrecursorIndex.Append(precursorIndex);
        if (ionMobility.HasValue) IonMobility.Append(ionMobility.Value);
        else IonMobility.AppendNull();
        if (ionMobilityType != null) IonMobilityType.Append(ionMobilityType);
        else IonMobilityType.AppendNull();
        Visited.Clear();
        VisitParameters(parameters);
    }

    public override void AppendNull()
    {
        SourceIndex.AppendNull();
        PrecursorIndex.AppendNull();
        IonMobility.AppendNull();
        IonMobilityType.AppendNull();
        base.AppendNull();
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>()
        {
            new Field("source_index", new UInt64Type(), true),
            new Field("precursor_index", new UInt64Type(), true),
            new Field("ion_mobility", new DoubleType(), true),
            new Field("ion_mobility_type", new StringType(), true)
        };
        foreach(var vis in ParamVisitors)
        {
            fields.AddRange(vis.ArrowType());
        }
        fields.AddRange(ParamList.ArrowType());
        return new() { new Field("selected_ion", new StructType(fields), true) };
    }

    public List<IArrowArray> Build()
    {
        var tp = ArrowType()[0];
        List<IArrowArray> fields = new() { SourceIndex.Build(), PrecursorIndex.Build(), IonMobility.Build(), IonMobilityType.Build() };
        foreach(var vis in ParamVisitors)
        {
            fields.AddRange(vis.Build());
        }
        fields.AddRange(ParamList.Build());
        var size = SourceIndex.Length;
        return new(){new StructArray(tp.DataType, size, fields, default)};
    }

    public void Clear()
    {
        SourceIndex.Clear();
        PrecursorIndex.Clear();
        IonMobility.Clear();
        IonMobilityType.Clear();
        foreach (var vis in ParamVisitors)
        {
            vis.Clear();
        }
        ParamList.Clear();
    }
}

public class ChromatogramBuilder : ParamVisitorCollection, IArrowBuilder<(ulong, string, string?, List<Param>, List<AuxiliaryArray>)>
{
    UInt64Array.Builder Index;
    StringArray.Builder Id;
    StringArray.Builder DataProcessingRef;
    Int32Array.Builder NumberOfAuxiliaryArrays;
    AuxiliaryArrayListBuilder AuxiliaryArrays;

    public int Length => Index.Length;

    public ChromatogramBuilder() : base(new()
    {
        // Required CV terms
        new CustomBuilderFromParam("MS:1000465", "scan polarity", new Int64Type()),
        new CustomBuilderFromParam("MS:1000626", "chromatogram type", new StringType()),
        // Optional properties (commonly present)
        new CustomBuilderFromParam("MS:1003060", "number of data points", new Int64Type()),
    })
    {
        Index = new();
        Id = new();
        DataProcessingRef = new();
        NumberOfAuxiliaryArrays = new();
        AuxiliaryArrays = new();
    }

    public void Append((ulong, string, string?, List<Param>, List<AuxiliaryArray>) value)
    {
        Append(value.Item1, value.Item2, value.Item3, value.Item4, value.Item5);
    }

    public void Append(ulong index, string id, string? dataProcessingRef, List<Param> parameters, List<AuxiliaryArray>? auxiliaryArrays = null)
    {
        Index.Append(index);
        Id.Append(id);
        if (dataProcessingRef != null) DataProcessingRef.Append(dataProcessingRef);
        else DataProcessingRef.AppendNull();
        NumberOfAuxiliaryArrays.Append(auxiliaryArrays?.Count ?? 0);
        AuxiliaryArrays.Append(auxiliaryArrays ?? []);
        Visited.Clear();
        VisitParameters(parameters);
    }

    public override void AppendNull()
    {
        Index.AppendNull();
        Id.AppendNull();
        DataProcessingRef.AppendNull();
        NumberOfAuxiliaryArrays.AppendNull();
        AuxiliaryArrays.AppendNull();
        base.AppendNull();
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>()
        {
            new Field("index", new UInt64Type(), true),
            new Field("id", new StringType(), true),
            new Field("data_processing_ref", new StringType(), true),
            new Field("number_of_auxiliary_arrays", new Int32Type(), true),
            new Field("auxiliary_arrays", AuxiliaryArrays.ArrowType()[0].DataType, true)
        };
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.ArrowType());
        }
        fields.AddRange(ParamList.ArrowType());
        return new() { new Field("chromatogram", new StructType(fields), true) };
    }

    public List<IArrowArray> Build()
    {
        List<IArrowArray> fields = new()
        {
            Index.Build(),
            Id.Build(),
            DataProcessingRef.Build(),
            NumberOfAuxiliaryArrays.Build(),
            AuxiliaryArrays.Build()[0]
        };
        foreach (var vis in ParamVisitors)
        {
            fields.AddRange(vis.Build());
        }
        fields.AddRange(ParamList.Build());
        var size = Index.Length;

        return new() { new StructArray(ArrowType()[0].DataType, size, fields, default) };
    }

    public void Clear()
    {
        Index.Clear();
        Id.Clear();
        DataProcessingRef.Clear();
        NumberOfAuxiliaryArrays.Clear();
        AuxiliaryArrays.Clear();
        foreach (var vis in ParamVisitors)
        {
            vis.Clear();
        }
        ParamList.Clear();
    }
}