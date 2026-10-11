namespace MZPeakNet.ShimadzuWrapper;

using Apache.Arrow;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using MZPeak.Writer;
using ParquetSharp;
using Shimadzu.LabSolutions.IO.Generic;
using Shimadzu.LabSolutions.IO.Generic.Param.MS;
using ShimadzuIO = Shimadzu.LabSolutions.IO;


public class ConversionContextHelper
{
    public ShimadzuIO.Data.DataObject? Module;
    public static readonly double MassMultiplier = 1.0 / Tool.MASSNUMBER_UNIT;
    public const double PrecursorMzMultiplier = 1.0 / 1e9;
    public const double TimeMultiplier = 0.001;

    Dictionary<short, MassEventInfo> Events;

    public ConversionContextHelper(ShimadzuIO.Data.DataObject? moduleObject=null)
    {
        Module = moduleObject;
        Events = new();
    }

    public void Initialize(ShimadzuIO.Data.DataObject moduleObject)
    {
        Module = moduleObject;

        List<ShimadzuIO.Generic.Param.MS.MassEventInfo> events;
        Module.MS.Parameters.GetEventInfo(out events);
        foreach (var evt in events)
        {
            Events.Add(evt.Event, evt);
        }
    }

    public (DoubleArray, FloatArray) GetSpectrumData(uint scanNumber, bool preferProfile = true)
    {
        if (Module == null) throw new InvalidDataException();
        ShimadzuIO.Generic.MassSpectrumObject spectrum;
        Module.MS.Spectrum.GetMSSpectrumByScan(out spectrum, scanNumber, preferProfile);
        if (preferProfile)
        {
            var mzArray = new DoubleArray.Builder();
            var intArray = new FloatArray.Builder();
            mzArray.Reserve(spectrum.ProfileList.Count);
            intArray.Reserve(spectrum.ProfileList.Count);
            foreach(var pt in spectrum.ProfileList)
            {
                mzArray.Append(pt.Mass * MassMultiplier);
                intArray.Append(pt.Intensity);
            }
            return (mzArray.Build(), intArray.Build());
        }
        else
        {
            var mzArray = new DoubleArray.Builder();
            var intArray = new FloatArray.Builder();
            mzArray.Reserve(spectrum.CentroidList.Count);
            intArray.Reserve(spectrum.CentroidList.Count);
            foreach (var pt in spectrum.CentroidList)
            {
                mzArray.Append(pt.Mass * MassMultiplier);
                intArray.Append(pt.Intensity);
            }
            return (mzArray.Build(), intArray.Build());
        }
    }

    public SpectrumDescription GetSpectrumDescription(uint scanNumber)
    {
        if (Module == null) throw new InvalidDataException();
        var info = SpectrumInfo.Empty;
        var rt = 0;
        var msLevel = 0;
        var precursorMz = 0;
        var precursorIdx = 0u;
        var polarity = Polarities.None;
        var segmentNo = 0;
        var eventNo = 0;
        var result = Module.MS.Spectrum.GetMSSpectrumInfo(scanNumber, out rt, out msLevel, out precursorMz, out precursorIdx, out polarity, out segmentNo, out eventNo);
        if (Tool.Failed(result))
        {
            throw new InvalidDataException($"Failed to read scan {scanNumber}: {result}");
        }
        info.Index = scanNumber;

        // MassEventInfo? evt = null;
        // Events.TryGetValue((short)eventNo, out evt);
        List<Param> scanParams = [
            ScanAttribute.PresetScanConfiguration.Param(segmentNo),
            ScanAttribute.ScanStartTime.Param(rt * TimeMultiplier),
        ];
        info.Time = rt * TimeMultiplier;
        info.MSLevel = (byte)msLevel;
        switch (polarity)
        {
            case Polarities.Negative:
                info.Parameters.Add(new Param(name: ScanPolarity.NegativeScan.Name(), accession: ScanPolarity.NegativeScan.CURIE(), null));
                break;
            case Polarities.Positive:
                info.Parameters.Add(new Param(name: ScanPolarity.PositiveScan.Name(), accession: ScanPolarity.PositiveScan.CURIE(), null));
                break;
            default:
                break;
        }

        var scan = new ScanInfo(scanNumber, null, parameters: scanParams);
        return new SpectrumDescription(info, [scan], [], [], null);
    }

    public List<InstrumentConfiguration> GetInstrumentConfigurations()
    {
        if (Module == null) throw new InvalidDataException();
        return [];
    }

    public Sample GetSample()
    {
        var sinfo = Module?.SampleInfo;
        if (sinfo == null) throw new InvalidDataException("No sample information");

        var sample = new Sample
        {
            Id=sinfo.SampleID, Name=sinfo.SampleName, Parameters = []
        };
        sample.Parameters.AddRange([
                    new Param("sample vial", sinfo.VialNumber),
            new Param("sample barcode", sinfo.Barcode),
            new Param("thermo:sample type", sinfo.SampleType.ToString()),
            new Param("thermo:dilution factor", sinfo.DilutionFactor),
            new Param(
                    SampleProperties.SampleVolume.Name(),
                    SampleProperties.SampleVolume.CURIE(),
                    sinfo.InjectionVolume,
                    Unit.Milliliter.CURIE()),
            new Param(
                    SampleProperties.SampleMass.Name(),
                    SampleProperties.SampleMass.CURIE(),
                    sinfo.SampleAmount,
                    Unit.Gram.CURIE()),
        ]);
        return sample;
    }
}

class CustomizedMZPeakWriter : MZPeakWriter, IDisposable
{

    public CustomizedMZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex? spectrumArrayIndex = null, ArrayIndex? chromatogramArrayIndex = null,
                                  ArrayIndex? spectrumPeakArrayIndex = null,
                                  SpectrumDataWriterOptions? spectrumDataOptions = null,
                                  SpectrumPeakDataWriterOptions? spectrumPeakDataOptions = null,
                                  ChromatogramDataWriterOptions? chromatogramDataOptions = null,
                                  Dictionary<string, FileEncryptionProperties>? encryptionConfigurations = null, ParquetDataWriterConfig? dataWriterConfig = null) :
                                  base(storage, spectrumArrayIndex, chromatogramArrayIndex, spectrumPeakArrayIndex,
                                       spectrumDataOptions, spectrumPeakDataOptions, chromatogramDataOptions,
                                       encryptionConfigurations, dataWriterConfig)
    {
    }


}

public class ShimadzuMZPeakWriter : IDisposable
{
    MZPeakWriter Writer;
    Dictionary<int, ulong> ScanNumberToIndex;
    public ConversionContextHelper ConversionHelper { get; protected set; }

    /// <summary>
    /// Set the current grid policy for the provided array type for mass spectrum profile.
    ///
    /// These policies are reset after the next write.
    /// </summary>
    /// <param name="arrayType"></param>
    /// <param name="policy"></param>
    public void SetSpectrumGridPolicy(ArrayType arrayType, GridPolicy? policy) => Writer.SetSpectrumGridPolicy(arrayType, policy);

    /// <summary>
    /// Set the current grid policy for the provided array type for mass spectrum peaks.
    ///
    /// These policies are reset after the next write.
    /// </summary>
    /// <param name="arrayType"></param>
    /// <param name="policy"></param>
    public void SetSpectrumPeakGridPolicy(ArrayType arrayType, GridPolicy? policy) => Writer.SetSpectrumPeakGridPolicy(arrayType, policy);

    /// <summary>
    /// Set the current grid policy for the provided array type for chromatogram.
    ///
    /// These policies are reset after the next write.
    /// </summary>
    /// <param name="arrayType"></param>
    /// <param name="policy"></param>
    public void SetChromatogramGridPolicy(ArrayType arrayType, GridPolicy? policy) => Writer.SetChromatogramGridPolicy(arrayType, policy);

    public ulong CurrentSpectrum => Writer.CurrentSpectrum;
    public ulong CurrentChromatogram => Writer.CurrentChromatogram;

    protected static ArrayIndex DefaultSpectrumArrayIndex(IDataWriterOptions options)
    {
        var builder = options.CreateArrayIndexBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts, transform: options.IntensityTransform());
        return builder.Build();
    }

    public static ArrayIndex PeakArrayIndex(bool includeResolution = false, bool includeCharge = false)
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        builder.Add(ArrayType.BaselineArray, BinaryDataType.Float32);
        builder.Add(ArrayType.NoiseArray, BinaryDataType.Float32);
        builder.Add(new ArrayIndexEntry()
        {
            ArrayName = "peak option flags array",
            ArrayTypeCURIE = ArrayType.NonStandardDataArray.CURIE(),
            Context = BufferContext.Spectrum,
            DataTypeCURIE = BinaryDataType.Int32.CURIE(),
            Path = "point.peak_option_flags",
            BufferFormat = BufferFormat.Point,
        });
        if (includeResolution)
            builder.Add(ArrayType.ResolutionArray, BinaryDataType.Int32);
        if (includeCharge)
            builder.Add(ArrayType.ChargeArray, BinaryDataType.Int32);

        return builder.Build();
    }

    public ArrayIndex SpectrumArrayIndex => Writer.SpectrumArrayIndex;
    public ArrayIndex ChromatogramArrayIndex => Writer.ChromatogramArrayIndex;
    public ArrayIndex? SpectrumPeakArrayIndex => Writer.SpectrumPeakArrayIndex;

    public ParquetDataWriterConfig DataWriterConfig { get => Writer.DataWriterConfig; set => Writer.DataWriterConfig = value; }

    public void SpectraUseNullMarking() => Writer.SpectraUseNullMarking();

    public ShimadzuMZPeakWriter(IMZPeakArchiveWriter storage,
                              ArrayIndex? spectrumArrayIndex = null,
                              ArrayIndex? chromatogramArrayIndex = null,
                              ArrayIndex? spectrumPeakArrayIndex = null,
                              bool includeNoise = false,
                              SpectrumDataWriterOptions? spectrumDataOptions = null,
                              SpectrumPeakDataWriterOptions? spectrumPeakDataOptions = null,
                              ChromatogramDataWriterOptions? chromatogramDataOptions = null,
                              Dictionary<string, ParquetSharp.FileEncryptionProperties>? encryptionConfigurations = null,
                              ParquetDataWriterConfig? dataWriterConfig = null)
    {
        spectrumDataOptions ??= new();
        spectrumArrayIndex ??= DefaultSpectrumArrayIndex(spectrumDataOptions);
        Writer = new CustomizedMZPeakWriter(
            storage,
            spectrumArrayIndex,
            chromatogramArrayIndex,
            spectrumPeakArrayIndex,
            spectrumDataOptions,
            spectrumPeakDataOptions,
            chromatogramDataOptions,
            encryptionConfigurations,
            dataWriterConfig);
        ScanNumberToIndex = new();
        ConversionHelper = new();
    }

    public void InitializeHelper(ShimadzuIO.Data.DataObject accessor)
    {
        ConversionHelper.Initialize(accessor);

        Samples = [ConversionHelper.GetSample()];
        InstrumentConfigurations = ConversionHelper.GetInstrumentConfigurations();
        // FileDescription = ConversionHelper.GetFileDescription(accessor);
        // Softwares = ConversionHelper.GetSoftwares(accessor);
        // DataProcessingMethods = [ConversionHelper.GetDataProcessingMethod(accessor)];

        // Run.DefaultDataProcessingId = DataProcessingMethods.Last().Id;
        // Run.DefaultSourceFileId = FileDescription.SourceFiles[0].Id;
        // Run.DefaultInstrumentId = (int)InstrumentConfigurations[0].Id;
        Run.StartTime = accessor.SampleInfo.AnalysisDate;
    }


    public void CloseCurrentWriter() => Writer.CloseCurrentWriter();
    public void FlushSpectrumData() => Writer.FlushSpectrumData();
    public void FlushSpectrumPeakData() => Writer.FlushSpectrumPeakData();

    /// <summary>Starts writing spectrum peak data.</summary>
    public void StartSpectrumPeakData(bool useTmp = false) => Writer.StartSpectrumPeakData(useTmp);

    /// <summary>Writes spectrum metadata to the archive.</summary>
    public void WriteSpectrumMetadata() => Writer.WriteSpectrumMetadata();

    public void WriteChromatogramData() => Writer.WriteChromatogramData();
    public void WriteChromatogramMetadata() => Writer.WriteChromatogramMetadata();

    /// <summary>Closes the writer and finalizes the archive.</summary>
    public void Close()
    {
        Writer.FlushStandardContent();
        Writer.Close();
    }

    public void Dispose() => Close();

    /// <summary>Gets or sets the file description metadata.</summary>
    public FileDescription FileDescription { get => Writer.FileDescription; set => Writer.FileDescription = value; }
    /// <summary>Gets or sets the list of instrument configurations.</summary>
    public List<InstrumentConfiguration> InstrumentConfigurations { get => Writer.InstrumentConfigurations; set => Writer.InstrumentConfigurations = value; }
    /// <summary>Gets or sets the list of software used.</summary>
    public List<Software> Softwares { get => Writer.Softwares; set => Writer.Softwares = value; }
    /// <summary>Gets or sets the list of samples.</summary>
    public List<Sample> Samples { get => Writer.Samples; set => Writer.Samples = value; }
    /// <summary>Gets or sets the list of data processing methods.</summary>
    public List<DataProcessingMethod> DataProcessingMethods { get => Writer.DataProcessingMethods; set => Writer.DataProcessingMethods = value; }
    /// <summary>Gets or sets the run-level metadata.</summary>
    public MSRun Run { get => Writer.Run; set => Writer.Run = value; }
}