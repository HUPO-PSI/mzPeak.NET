using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using MZPeak.Writer;
using ThermoFisher.CommonCore.Data;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.FilterEnums;
using ThermoFisher.CommonCore.Data.Interfaces;

namespace MZPeak.Thermo;


public struct IsolationWindow
{
    public double LowerMZ;
    public double TargetMZ;
    public double UpperMZ;

    public IsolationWindow(double isolationWidth, double monoisotopicMZ, double isolationOffset)
    {
        LowerMZ = monoisotopicMZ + isolationOffset - isolationWidth;
        UpperMZ = monoisotopicMZ + isolationOffset + isolationWidth;
        TargetMZ = monoisotopicMZ;
    }

    public List<Param> ToParamList()
    {
        return new()
        {
            new Param(
                IsolationWindowProperties.IsolationWindowTargetMZ.Name(),
                IsolationWindowProperties.IsolationWindowTargetMZ.CURIE(),
                TargetMZ,
                Unit.MZ.CURIE()
            ),
            new Param(
                IsolationWindowProperties.IsolationWindowLowerLimit.Name(),
                IsolationWindowProperties.IsolationWindowLowerLimit.CURIE(),
                LowerMZ,
                Unit.MZ.CURIE()
            ),
            new Param(
                IsolationWindowProperties.IsolationWindowUpperLimit.Name(),
                IsolationWindowProperties.IsolationWindowUpperLimit.CURIE(),
                UpperMZ,
                Unit.MZ.CURIE()
            ),
        };
    }
}

public record ActivationProperties(ActivationType Dissociation, double Energy)
{
    public List<Param> AsParamList()
    {
        var dissociation = Dissociation switch
        {
            ActivationType.CollisionInducedDissociation => DissociationMethod.CollisionInducedDissociation,
            ActivationType.ElectronCaptureDissociation => DissociationMethod.ElectronCaptureDissociation,
            ActivationType.MultiPhotonDissociation => DissociationMethod.Photodissociation,
            ActivationType.ElectronTransferDissociation => DissociationMethod.ElectronTransferDissociation,
            ActivationType.HigherEnergyCollisionalDissociation => DissociationMethod.BeamTypeCollisionInducedDissociation,
            ActivationType.NegativeElectronTransferDissociation => DissociationMethod.NegativeElectronTransferDissociation,
            ActivationType.UltraVioletPhotoDissociation => DissociationMethod.UltravioletPhotodissociation,
            _ => throw new NotImplementedException($"{Dissociation} not mapped"),
        };

        return new()
        {
            new Param(dissociation.Name(), accession: dissociation.CURIE(), rawValue: null),
            new Param("collision energy", "MS:1000045", Energy, Unit.Volt.CURIE())
        };
    }
}

public record PrecursorProperties(double MonoisotopicMZ, int PrecursorCharge, IsolationWindow IsolationWindow, int MasterScanNumber, ActivationProperties Activation)
{ }


public record AcquisitionProperties(
    double InjectionTime,
    MassAnalyzerType Analyzer,
    IonizationModeType Ionization,
    double LowMZ,
    double HighMZ,
    int ScanEventNumber,
    float? Resolution)
{ }


public class ConversionContextHelper
{
    private const string InjectionTimeKey = "Ion Injection Time (ms)";
    private const string ScanEventKey = "Scan Evnet";
    private const string MasterScanKey = "Master Scan";
    private const string MonoisotopicMZKey = "Monoisotopic M/Z";
    private const string ChargeStateKey = "Charge State";
    private static readonly string[] IsolationLevelKeys = [
        "MS2 Isolation Width",
        "MS3 Isolation Width",
        "MS4 Isolation Width",
        "MS5 Isolation Width",
        "MS6 Isolation Width",
        "MS7 Isolation Width",
        "MS8 Isolation Width",
        "MS9 Isolation Width",
        "MS10 Isolation Width"
    ];

    private static readonly string OrbitrapResolutionKey = "Orbitrap Resolution";
    private static readonly string FTResolutionKey = "FT Resolution";
    public static Dictionary<MSOrderType, int> MSLevelMap = new Dictionary<MSOrderType, int>() {
            {MSOrderType.Ms, 1},
            {MSOrderType.Ms2, 2},
            {MSOrderType.Ms3, 3},
            {MSOrderType.Ms4, 4},
            {MSOrderType.Ms5, 5},
            {MSOrderType.Ms6, 6},
            {MSOrderType.Ms7, 7},
            {MSOrderType.Ms8, 8},
            {MSOrderType.Ms9, 9},
            {MSOrderType.Ms10, 10},
            {MSOrderType.Ng, 2},
            {MSOrderType.Nl, 2},
            {MSOrderType.Par, 2},
        };

    /// <summary>
    /// An index look up mapping trailer keys by index that lets us avoid
    /// looping over all trailer entries
    /// </summary>
    public Dictionary<string, int> TrailerMap;
    public List<HeaderItem> Headers;
    public Dictionary<int, List<int?>> PreviousMSLevels;
    public Dictionary<int, uint> MSLevelCounts;

    public ConversionContextHelper()
    {
        TrailerMap = new();
        Headers = new();
        PreviousMSLevels = new();
        MSLevelCounts = new();
    }

    public bool GetShortTrailerExtraFor(IRawDataPlus accessor, int scanNumber, string key, out short value)
    {
        object tmp;
        HeaderItem header;
        int headerIdx;

        if (TrailerMap.TryGetValue(key, out headerIdx))
        {
            tmp = accessor.GetTrailerExtraValue(scanNumber, headerIdx);
            header = Headers[headerIdx];
            if (tmp != null)
            {
                try
                {
                    switch (header.DataType)
                    {
                        case GenericDataTypes.SHORT:
                            {
                                value = (short)tmp;
                                return true;
                            }
                        case GenericDataTypes.LONG:
                            {
                                value = Convert.ToInt16(tmp);
                                return true;
                            }
                        case GenericDataTypes.ULONG:
                            {
                                value = Convert.ToInt16(tmp);
                                return true;
                            }
                        case GenericDataTypes.USHORT:
                            {
                                value = (short)(ushort)tmp;
                                return true;
                            }
                        default:
                            {
                                value = Convert.ToInt16(tmp);
                                return true;
                            }
                    }
                }
                catch (InvalidCastException)
                {
                    value = Convert.ToInt16(tmp);
                    return true;
                }

            }
        }
        value = 0;
        return false;
    }

    public bool GetIntTrailerExtraFor(IRawDataPlus accessor, int scanNumber, string key, out int value, int defaultValue = 0)
    {
        object tmp;
        HeaderItem header;
        int headerIdx;

        if (TrailerMap.TryGetValue(key, out headerIdx))
        {
            tmp = accessor.GetTrailerExtraValue(scanNumber, headerIdx);
            header = Headers[headerIdx];
            if (tmp != null)
            {
                try
                {
                    switch (header.DataType)
                    {
                        case GenericDataTypes.SHORT:
                            {
                                value = (short)tmp;
                                return true;
                            }
                        case GenericDataTypes.LONG:
                            {
                                value = (int)(long)tmp;
                                return true;
                            }
                        case GenericDataTypes.ULONG:
                            {
                                value = (int)(ulong)tmp;
                                return true;
                            }
                        case GenericDataTypes.USHORT:
                            {
                                value = (ushort)tmp;
                                return true;
                            }
                        default:
                            {
                                value = Convert.ToInt32(tmp);
                                return true;
                            }
                    }
                }
                catch (InvalidCastException)
                {
                    value = Convert.ToInt32(tmp);
                    return true;
                }
            }
        }
        value = defaultValue;
        return false;
    }

    public bool GetDoubleTrailerExtraFor(IRawDataPlus accessor, int scanNumber, string key, out double value)
    {
        object tmp;
        HeaderItem header;
        int headerIdx;

        if (TrailerMap.TryGetValue(key, out headerIdx))
        {
            tmp = accessor.GetTrailerExtraValue(scanNumber, headerIdx);
            header = Headers[headerIdx];
            if (tmp != null)
            {
                try
                {
                    switch (header.DataType)
                    {
                        case GenericDataTypes.FLOAT:
                            {
                                value = (float)tmp;
                                return true;
                            }
                        case GenericDataTypes.DOUBLE:
                            {
                                value = (double)tmp;
                                return true;
                            }
                            ;
                        default:
                            {
                                value = Convert.ToDouble(tmp);
                                return true;
                            }
                    }
                }
                catch (InvalidCastException)
                {
                    value = Convert.ToDouble(tmp);
                    return true;
                }
            }
        }
        value = 0;
        return false;
    }

    private void BuildScanTypeMap(IRawDataPlus accessor)
    {
        Dictionary<int, uint> msLevelCounts = new() {
                {1, 0},
                {2, 0},
                {3, 0},
                {4, 0},
                {5, 0},
                {6, 0},
                {7, 0},
                {8, 0},
                {9, 0},
                {10, 0},
            };
        Dictionary<int, List<int?>> previousMSLevels = new();
        Dictionary<int, int?> lastMSLevels = new() {
                {1, null},
                {2, null},
                {3, null},
                {4, null},
                {5, null},
                {6, null},
                {7, null},
                {8, null},
                {9, null},
                {10, null},
            };

        var last = accessor.RunHeaderEx.LastSpectrum;
        for (var i = accessor.RunHeaderEx.FirstSpectrum; i <= last; i++)
        {
            var filter = accessor.GetFilterForScanNumber(i);
            var msLevel = MSLevelMap[filter.MSOrder]; ;

            msLevelCounts[msLevel] += 1;

            List<int?> backwards = new();
            for (short j = 1; j < msLevel + 1; j++)
            {
                var o = lastMSLevels[j];
                backwards.Add(o);
            }
            previousMSLevels[i] = backwards;

            lastMSLevels[msLevel] = i;
        }

        PreviousMSLevels = previousMSLevels;
        MSLevelCounts = msLevelCounts;
    }

    public void Initialize(IRawDataPlus accessor)
    {
        var headers = accessor.GetTrailerExtraHeaderInformation();
        for (var i = 0; i < headers.Length; i++)
        {
            var header = headers[i];
            var label = header.Label.TrimEnd(':');
            TrailerMap[label] = i;
        }
        Headers = headers.ToList();
        BuildScanTypeMap(accessor);
    }

    public Dictionary<(MassAnalyzerType, IonizationModeType), long> FindAllMassAnalyzers(IRawDataPlus accessor)
    {
        var analyzers = new Dictionary<(MassAnalyzerType, IonizationModeType), long>();
        var events = accessor.ScanEvents;

        int counter = 0;
        for (var segmentIdx = 0; segmentIdx < events.Segments; segmentIdx++)
        {
            for (var eventIdx = 0; eventIdx < events.GetEventCount(segmentIdx); eventIdx++)
            {
                var ev = events.GetEvent(segmentIdx, eventIdx);
                var a = ev.MassAnalyzer;
                var i = ev.IonizationMode;
                if (analyzers.ContainsKey((a, i)))
                {
                    continue;
                }
                analyzers.Add((a, i), counter);
                counter += 1;
            }
        }
        return analyzers;
    }

    public ActivationProperties ExtractActivation(int msLevel, IScanFilter filter)
    {
        ActivationProperties activation = new ActivationProperties(
            filter.GetActivation(msLevel - 2),
            filter.GetEnergy(msLevel - 2)
        );
        return activation;
    }

    /// <summary>
    /// Find the scan number of the precursor, which is assumed to be the most recent spectrum of a lower
    /// MS level, if it was not indicated some other way. This method is used when the Master Scan Number was
    /// not set in a trailer value.
    /// </summary>
    /// <param name="scanNumber">The scan number to search back from</param>
    /// <param name="msLevel">The MS level to search for lesser values from</param>
    /// <param name="accessor">The current RAW file accessor</param>
    /// <returns>The scan number of the most recent lower MS level spectrum</returns>
    int FindPreviousPrecursor(int scanNumber, int msLevel, IRawDataPlus accessor)
    {
        var cacheLookUp = PreviousMSLevels[scanNumber][msLevel - 1];
        if (cacheLookUp != null)
        {
            return cacheLookUp.Value;
        }
        int i = scanNumber - 1;
        while (i > 0)
        {
            var filter = accessor.GetFilterForScanNumber(i);
            var levelOf = MSLevelMap[filter.MSOrder];
            if (levelOf < msLevel)
            {
                return i;
            }
            else
            {
                i -= 1;
            }
        }

        return i;
    }

    public (PrecursorProperties?, AcquisitionProperties) ExtractPrecursorAndTrailerMetadata(int scanNumber, int msLevel, IScanFilter filter, IRawDataPlus accessor, ScanStatistics stats)
    {
        var trailers = accessor.GetTrailerExtraInformation(scanNumber);

        var n = trailers.Length;
        double monoisotopicMZ = 0.0;
        short precursorCharge = 0;
        double isolationWidth = 0.0;
        double injectionTime = 0.0;
        int masterScanNumber = -1;
        short scanEventNum = 1;
        double resolution = 0.0;
        float? resolution_opt = null;

        GetDoubleTrailerExtraFor(accessor, scanNumber, InjectionTimeKey, out injectionTime);
        GetShortTrailerExtraFor(accessor, scanNumber, ScanEventKey, out scanEventNum);

        if (msLevel > 1)
        {
            GetIntTrailerExtraFor(accessor, scanNumber, MasterScanKey, out masterScanNumber, -1);
            GetDoubleTrailerExtraFor(accessor, scanNumber, MonoisotopicMZKey, out monoisotopicMZ);
            GetShortTrailerExtraFor(accessor, scanNumber, ChargeStateKey, out precursorCharge);
            GetDoubleTrailerExtraFor(accessor, scanNumber, IsolationLevelKeys[msLevel - 2], out isolationWidth);
        }

        if (!GetDoubleTrailerExtraFor(accessor, scanNumber, OrbitrapResolutionKey, out resolution))
        {
            GetDoubleTrailerExtraFor(accessor, scanNumber, FTResolutionKey, out resolution);
        }
        ;
        resolution_opt = resolution == 0.0 ? null : (float)resolution;

        AcquisitionProperties acquisitionProperties = new AcquisitionProperties(injectionTime, filter.MassAnalyzer, IonizationModeType.Any, stats.LowMass, stats.HighMass, scanEventNum, resolution_opt);

        if (msLevel > 1 && isolationWidth == 0.0)
        {
            isolationWidth = filter.GetIsolationWidth(msLevel - 2) / 2;
        }
        if (msLevel > 1)
        {
            double isolationOffset = filter.GetIsolationWidthOffset(msLevel - 2);
            if (monoisotopicMZ == 0.0)
            {
                monoisotopicMZ = filter.GetMass(msLevel - 2);
            }

            if (masterScanNumber == -1)
            {
                masterScanNumber = FindPreviousPrecursor(scanNumber, msLevel, accessor);
            }

            ActivationProperties activation = ExtractActivation(msLevel, filter);
            IsolationWindow window = new IsolationWindow(isolationWidth, monoisotopicMZ, isolationOffset);
            PrecursorProperties props = new PrecursorProperties(monoisotopicMZ, precursorCharge, window, masterScanNumber, activation);
            return (props, acquisitionProperties);
        }
        else
        {
            return (null, acquisitionProperties);
        }
    }
}


public class ThermoMZPeakWriter
{
    MZPeakWriter Writer;
    Dictionary<ulong, int> ScanNumberToIndex;
    ConversionContextHelper ConversionHelper;

    protected static ArrayIndex DefaultSpectrumArrayIndex()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    public ThermoMZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex? spectrumArrayIndex = null, ArrayIndex? chromatogramArrayIndex = null)
    {
        if (spectrumArrayIndex == null)
        {
            spectrumArrayIndex = DefaultSpectrumArrayIndex();
        }
        Writer = new MZPeakWriter(storage, spectrumArrayIndex);
        ScanNumberToIndex = new();
        ConversionHelper = new();
    }

    public void InitializeHelper(IRawDataPlus accessor)
    {
        ConversionHelper.Initialize(accessor);
    }

    Param GetMSLevel(IScanFilter scanFilter)
    {
        var msLevel = ConversionContextHelper.MSLevelMap[scanFilter.MSOrder];
        return new Param(SpectrumProperties.MsLevel.Name(), SpectrumProperties.MsLevel.CURIE(), msLevel);
    }

    Param GetPolarity(IScanFilter scanFilter)
    {
        switch (scanFilter.Polarity)
        {
            case PolarityType.Negative:
                {
                    return new Param(ScanPolarity.ScanPolarity.Name(), ScanPolarity.ScanPolarity.CURIE(), -1);
                }
            case PolarityType.Positive:
                {
                    return new Param(ScanPolarity.ScanPolarity.Name(), ScanPolarity.ScanPolarity.CURIE(), 1);
                }
            case PolarityType.Any:
                {
                    return new Param(ScanPolarity.ScanPolarity.Name(), ScanPolarity.ScanPolarity.CURIE(), 0);
                }
            default: throw new InvalidOperationException();
        }
    }

    public ulong AddSpectrum(
        int scanNumber,
        double time,
        IScanFilter scanFilter,
        ScanStatistics scanStatistics,
        List<double>? spacingModel = null,
        List<Param>? @params = null,
        List<AuxiliaryArray>? auxiliaryArrays = null)
    {
        List<Param> paramList = @params ?? new();
        paramList.Add(GetMSLevel(scanFilter));
        paramList.Add(GetPolarity(scanFilter));
        paramList.Add(new Param(
            SpectrumProperties.BasePeakIntensity.Name(),
            SpectrumProperties.BasePeakIntensity.CURIE(),
            scanStatistics.BasePeakIntensity,
            Unit.NumberOfDetectorCounts.CURIE()
        ));
        paramList.Add(new Param(
            SpectrumProperties.BasePeakMZ.Name(),
            SpectrumProperties.BasePeakMZ.CURIE(),
            scanStatistics.BasePeakMass,
            Unit.MZ.CURIE()
        ));
        paramList.Add(new Param(
            SpectrumProperties.TotalIonCurrent.Name(),
            SpectrumProperties.TotalIonCurrent.CURIE(),
            scanStatistics.TIC,
            Unit.NumberOfDetectorCounts.CURIE()
        ));
        if (scanStatistics.IsCentroidScan)
            paramList.Add(SpectrumRepresentation.CentroidSpectrum.AsParam());
        else
            paramList.Add(SpectrumRepresentation.ProfileSpectrum.AsParam());

        var id = $"controllerType=0 controllerNumber=1 scan={scanNumber + 1}";
        var index = Writer.AddSpectrum(id, time, null, spacingModel, paramList, auxiliaryArrays);
        ScanNumberToIndex[index] = scanNumber;
        return index;
    }

    public void AddScan(
        ulong sourceIndex,
        int scanNumber,
        double time,
        IScanFilter scanFilter,
        ScanStatistics scanStatistics,
        AcquisitionProperties acquisitionProperties,
        List<Param>? @params = null
    )
    {
        var instrumentRef = 0u;
        List<Param> paramList = @params ?? new();
        double? faims = null;
        string? imCV = null;

        paramList.AddRange([
            new(ScanAttribute.ScanStartTime.Name(),
                ScanAttribute.ScanStartTime.CURIE(),
                time,
                Unit.Minute.CURIE()),
            new(ScanAttribute.FilterString.Name(), ScanAttribute.FilterString.CURIE(), scanFilter.ToString()),
        ]);
        if (scanFilter.CompensationVoltageCount == 1)
        {
            faims = scanFilter.CompensationVoltageValue(0);
            imCV = ScanAttribute.FaimsCompensationVoltage.CURIE();
        }
        else if (scanFilter.CompensationVoltageCount > 1)
            throw new NotImplementedException("Multiple FAIMS CV not yet supported");

        List<ScanWindow> scanWindows = [new ScanWindow(acquisitionProperties.LowMZ, acquisitionProperties.HighMZ, Unit.MZ)];

        paramList.Add(new Param(
            ScanAttribute.FilterString.Name(),
            ScanAttribute.FilterString.CURIE(),
            scanFilter.ToString())
        );

        if (acquisitionProperties.Resolution != null)
            paramList.Add(new Param(
                ScanAttribute.MassResolution.Name(),
                ScanAttribute.MassResolution.CURIE(),
                acquisitionProperties.Resolution));
        paramList.Add(new Param(
            ScanAttribute.IonInjectionTime.Name(),
            ScanAttribute.IonInjectionTime.CURIE(),
            acquisitionProperties.InjectionTime,
            Unit.Millisecond.CURIE()
        ));

        Writer.AddScan(
            sourceIndex,
            instrumentRef,
            paramList,
            ionMobility: faims,
            ionMobilityType: imCV,
            scanWindows: scanWindows.Select(w => w.AsParamList()).ToList()
        );
    }

    public (PrecursorProperties?, AcquisitionProperties) ExtractPrecursorAndTrailerMetadata(int scanNumber, int msLevel, IRawDataPlus accessor, IScanFilter filter, ScanStatistics stats)
    {
        return ConversionHelper.ExtractPrecursorAndTrailerMetadata(scanNumber, msLevel, filter, accessor, stats);
    }

    public void AddPrecursor(
        ulong sourceIndex,
        ulong precursorIndex,
        PrecursorProperties precursorProperties,
        List<Param>? @activationParams = null
    )
    {
        var precursorScanNumber = ScanNumberToIndex[precursorIndex];
        var precursorId = $"controllerType=0 controllerNumber=1 scan={precursorScanNumber + 1}";
        var activationParamList = @activationParams ?? new();
        activationParamList.AddRange(precursorProperties.Activation.AsParamList());
        Writer.AddPrecursor(
            sourceIndex,
            precursorIndex,
            precursorId,
            precursorProperties.IsolationWindow.ToParamList(),
            activationParamList
        );
    }

    public void AddSelectedIonMetadata(
        ulong sourceIndex,
        ulong precursorIndex,
        PrecursorProperties precursorProperties
    )
    {
        List<Param> paramList = new()
        {
            new Param(
                IonSelectionProperties.ChargeState.Name(),
                IonSelectionProperties.ChargeState.CURIE(),
                rawValue: precursorProperties.PrecursorCharge
            ),
            new Param(
                IonSelectionProperties.SelectedIonMZ.Name(),
                IonSelectionProperties.SelectedIonMZ.CURIE(),
                precursorProperties.MonoisotopicMZ,
                Unit.MZ.CURIE()
            ),
        };
        Writer.AddSelectedIon(
            sourceIndex,
            precursorIndex,
            paramList
        );
    }
}
