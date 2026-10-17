namespace MZPeakNet.ShimadzuWrapper;

using Apache.Arrow;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using MZPeak.Writer;
using MZPeak.Writer.Data;
using ParquetSharp;
using Shimadzu.LabSolutions.IO.Generic;
using Shimadzu.LabSolutions.IO.Generic.Param.MS;
using ShimadzuIO = Shimadzu.LabSolutions.IO;

record struct PrecursorT(double PrecursorMz, int Charge, double CollisionEnergy);
public record SpectrumRecord(SpectrumDescription Description, List<IArrowArray>? ProfileArrays, List<IArrowArray>? CentroidArrays)
{
    public  EntryDerivedMetadata EntryDerivedMetadata() {
        return new(
            Description.CoordinateSpacingModelParameters != null ? new (Description.CoordinateSpacingModelParameters) : null,
            Description.AuxiliaryArrays,
            (int?) Description.DataPointCount,
            (int?) Description.PeakCount
        );
    }
};

public class ConversionContextHelper
{
    public ShimadzuIO.Data.DataObject? Module;
    public static readonly double MassMultiplier = 1.0 / Tool.MASSNUMBER_UNIT;
    public const double PrecursorMzMultiplier = 1.0 / 1e9;
    public const double TimeMultiplier = 0.001;

    Dictionary<short, MassEventInfo> Events;
    Dictionary<uint, PrecursorT> Precursors;

    public uint ScanCount {get; protected set;}
    public int SegmentCount { get; protected set; }

    public ConversionContextHelper(ShimadzuIO.Data.DataObject? moduleObject=null)
    {
        Module = moduleObject;
        Events = new();
        Precursors = new();
        ScanCount = 0;
        SegmentCount = 0;
    }

    public void Initialize(ShimadzuIO.Data.DataObject moduleObject)
    {
        Module = moduleObject;

        List<MassEventInfo> events;
        Module.MS.Parameters.GetEventInfo(out events);
        foreach (var evt in events)
        {
            Events.Add(evt.Event, evt);
        }

        List<List<short>> segments = [];
        var segmentCount = Module.MS.Chromatogram.SegmentCount;
        for(short seg = 1; seg <= segmentCount; seg++)
        {
            var ct = Module.MS.Chromatogram.EventCount(seg);
            List<short> block = [];
            for(short i = 1; i < ct; i++)
                block.Add(Module.MS.Chromatogram.GetEventNo(seg, i));
            segments.Add(block);
        }

        if (segmentCount == 0 && Events.Count > 0)
        {
            Dictionary<short, HashSet<short>> segmentMap = new();
            foreach(var (k, v) in Events)
            {
                HashSet<short>? block;
                if (segmentMap.TryGetValue(v.Segment, out block))
                {
                    block.Add(k);
                }
                else
                {
                    block = [k];
                    segmentMap[v.Segment] = block;
                }
            }
            if (segmentMap.Count > 0)
                segmentCount = segmentMap.Keys.Max();
            for(short seg = 1; seg <= segmentCount; seg++)
            {
                var block = segmentMap[seg].ToList();
                block.Sort();
                segments.Add(block);
            }
        }

        SegmentCount = segmentCount;

        ScanCount = Events.Values.Max(evt =>
        {
            uint val = 0;
            LastScanNumberOfEvent(evt.Event, evt.EndRt, out val);
            return val;
        });

        LoadPrecursors();
    }

    bool LastScanNumberOfEvent(short eventNo, int endTime, out uint scanNumber)
    {
        scanNumber = 0;
        var r = Module?.MS.Spectrum.RetTimeToScan(out scanNumber, endTime, eventNo);
        if (r != null) return !Tool.Failed(r.Value);
        else return false;
    }

    void LoadPrecursors()
    {
        if (Module == null) throw new InvalidDataException();
        PrecursorResultData? precursorResultData;
        var r = Module.MS.Spectrum.GetPrecursorList(default, out precursorResultData);
        if (Tool.Failed(r))
            throw new InvalidOperationException($"Precursor loading failed: {r}");
        if (precursorResultData.SurveyList.Count == 0) return;
        foreach(var dep in precursorResultData.SurveyList[0].DependentList)
        {
            int charge = dep.Charge switch
            {
                Charges.Charge1 => 1,
                Charges.Charge2 => 2,
                Charges.Charge3 => 3,
                Charges.Charge4 => 4,
                Charges.Charge5 => 5,
                Charges.Charge6 => 6,
                Charges.Charge7 => 7,
                Charges.Charge8 => 8,
                _ => 0,
            };
            foreach(uint s in dep.ScanNoList)
            {
                Precursors[s] = new PrecursorT(dep.PrecursorMass * PrecursorMzMultiplier, charge, dep.CollisionEnergy);
            }
        }
    }

    public (DoubleArray, FloatArray, MassSpectrumObject) GetSpectrumData(uint scanNumber, bool preferProfile = true)
    {
        if (Module == null) throw new InvalidDataException();
        MassSpectrumObject spectrum;
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
            return (mzArray.Build(), intArray.Build(), spectrum);
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
            return (mzArray.Build(), intArray.Build(), spectrum);
        }
    }

    public SpectrumRecord GetSpectrumDescription(uint scanNumber)
    {
        if (Module == null) throw new InvalidDataException();
        var info = SpectrumInfo.Empty;
        int rt;
        int msLevel;
        int precursorMz;
        uint precursorIdx;
        Polarities polarity;
        int segmentNo;
        int eventNo;
        var result = Module.MS.Spectrum.GetMSSpectrumInfo(scanNumber, out rt, out msLevel, out precursorMz, out precursorIdx, out polarity, out segmentNo, out eventNo);
        if (Tool.Failed(result))
        {
            throw new InvalidDataException($"Failed to read scan {scanNumber}: {result}");
        }
        info.Index = scanNumber;

        MassEventInfo? evt;
        Events.TryGetValue((short)eventNo, out evt);

        List<Param> scanParams = [
            ScanAttribute.PresetScanConfiguration.Param(segmentNo),
            ScanAttribute.ScanStartTime.Param(rt * TimeMultiplier),
        ];
        var scan = new ScanInfo(scanNumber, null, parameters: scanParams);

        if (evt != null)
            scan.ScanWindows.Add(new ScanWindow(evt.StartMz * MassMultiplier, evt.EndMz * MassMultiplier, Unit.MZ));

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

        List<PrecursorInfo> precursorList = [];
        List<SelectedIonInfo> selectedIonsList = [];
        if (msLevel > 1) {
            var precursor = new PrecursorInfo(scanNumber, precursorIdx, null, [], []);

            precursorList.Add(precursor);
            PrecursorT precursorRec;
            if (Precursors.TryGetValue(scanNumber, out precursorRec))
            {
                List<Param> siParams = [
                    IonSelectionProperties.ChargeState.Param(precursorRec.Charge),
                    IonSelectionProperties.SelectedIonMZ.Param(precursorRec.PrecursorMz)
                ];
                selectedIonsList.Add(new SelectedIonInfo(scanNumber, precursorIdx, parameters: siParams));
                precursor.ActivationParameters.Add(
                    new Param(
                        name: Activation.BeamTypeCollisionInducedDissociation.Name(),
                        accession: Activation.BeamTypeCollisionInducedDissociation.CURIE(),
                        rawValue: null));
                if (evt != null)
                {
                    var isolationWidthHalf = evt.QTransmissionMzWidthNmzManual * PrecursorMzMultiplier / 2;
                    precursor.IsolationWindowParameters.AddRange([
                        IsolationWindowProperties.IsolationWindowTargetMZ.Param(precursorRec.PrecursorMz, unit: Unit.MZ),
                        IsolationWindowProperties.IsolationWindowLowerOffset.Param(isolationWidthHalf, Unit.MZ),
                        IsolationWindowProperties.IsolationWindowUpperOffset.Param(isolationWidthHalf, Unit.MZ),
                    ]);

                }
            }
        }

        var (mzArrays, intensityArray, desc) = GetSpectrumData(scanNumber, true);
        List<IArrowArray>? profileArrays = [mzArrays, intensityArray];
        var isProfile = false;
        if (desc.ProfileList.Count > 0)
        {
            info.DataPointCount = desc.ProfileList.Count;
            info.Parameters.Add(SpectrumRepresentation.ProfileSpectrum.AsParam());
            isProfile = true;
        }
        else
        {
            profileArrays = null;
        }
        (mzArrays, intensityArray, desc) = GetSpectrumData(scanNumber, false);
        List<IArrowArray>? centroidArrays = [mzArrays, intensityArray];
        if (desc.CentroidList.Count > 0)
        {
            info.PeakCount = desc.CentroidList.Count;
            if (!isProfile) info.Parameters.Add(SpectrumRepresentation.CentroidSpectrum.AsParam());
        }
        else
        {
            centroidArrays = null;
        }

        var rec = new SpectrumRecord(
            new SpectrumDescription(info, [scan], precursorList, selectedIonsList, null),
            profileArrays,
            centroidArrays
        );
        return rec;
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
    Dictionary<uint, ulong> ScanNumberToIndex;
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

    public ulong AddSpectrumWithData(uint scanNumber, SpectrumRecord? rec = null)
    {
        if (rec == null)
            rec = ConversionHelper.GetSpectrumDescription(scanNumber);
        var currentIdx = Writer.CurrentSpectrum;
        ScanNumberToIndex[scanNumber] = currentIdx;
        EntryDerivedMetadata? dataEntry;
        var mzGrid = new LinearGrid(0.0, ConversionContextHelper.MassMultiplier);
        var mzGridPolicy = new GridPolicy(ArrayType.MZArray, false, 1e-6)
        {
            CurrentGrid = mzGrid
        };

        if (rec.Description.DataPointCount > 0 && rec.ProfileArrays != null)
        {
            Writer.SetSpectrumGridPolicy(ArrayType.MZArray, mzGridPolicy);
            dataEntry = Writer.AddSpectrumData(currentIdx, rec.ProfileArrays, true);
            rec.Description.DataPointCount = dataEntry.DataPointCount;
            rec.Description.CoordinateSpacingModelParameters = dataEntry.SpacingInterpolationModel?.Coefficients;
        }

        if (rec.Description.PeakCount > 0 && rec.CentroidArrays != null)
        {
            Writer.SetSpectrumPeakGridPolicy(ArrayType.MZArray, mzGridPolicy);
            var peakEntry = Writer.AddSpectrumPeakData(currentIdx, rec.CentroidArrays);
            rec.Description.PeakCount = peakEntry.PeakCount;
        }

        var indexNew = Writer.AddSpectrum(
            rec.Description.Id,
            rec.Description.Time,
            rec.Description.DataProcessingRef,
            rec.Description.Parameters,
            rec.EntryDerivedMetadata()
        );
        foreach(var scan in rec.Description.Scans)
        {
            Writer.AddScan(scan, indexNew);
        }
        foreach(var precursor in rec.Description.Precursors)
        {
            ulong? newPrecursorIdx = null;
            if (precursor.PrecursorIndex != null)
            {
                ulong tmp;
                if (ScanNumberToIndex.TryGetValue((uint)precursor.PrecursorIndex.Value, out tmp))
                    newPrecursorIdx = tmp;
            }
            Writer.AddPrecursor(precursor, indexNew, newPrecursorIdx);
        }
        foreach (var ion in rec.Description.SelectedIons)
        {
            ulong? newPrecursorIdx = null;
            if (ion.PrecursorIndex != null)
            {
                ulong tmp;
                if (ScanNumberToIndex.TryGetValue((uint)ion.PrecursorIndex.Value, out tmp))
                    newPrecursorIdx = tmp;
            }
            Writer.AddSelectedIon(selectedIon: ion, indexNew, newPrecursorIdx);
        }
        return indexNew;
    }

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