using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Arrow;

namespace MZPeak.ControlledVocabulary;

public enum ArrayType
{
    BinaryDataArray,
    MZArray,
    IntensityArray,
    ChargeArray,
    SignalToNoiseArray,
    TimeArray,
    WavelengthArray,
    NonStandardDataArray,
    FlowRateArray,
    PressureArray,
    TemperatureArray,
    MeanChargeArray,
    ResolutionArray,
    BaselineArray,
    NoiseArray,
    SampledNoiseMZArray,
    SampledNoiseIntensityArray,
    SampledNoiseBaselineArray,
    IonMobilityArray,
    MassArray,
    ScanningQuadrupolePositionLowerBoundMZArray,
    ScanningQuadrupolePositionUpperBoundMZArray,
    MeanIonMobilityDriftTimeArray,
    MeanIonMobilityArray,
    MeanInverseReducedIonMobilityArray,
    RawIonMobilityArray,
    RawInverseReducedIonMobilityArray,
    RawIonMobilityDriftTimeArray,
    DeconvolutedIonMobilityArray,
    DeconvolutedInverseReducedIonMobilityArray,
    DeconvolutedIonMobilityDriftTimeArray,
}

public static class ArrayTypeMethods
{
    public static readonly Dictionary<string, ArrayType> FromCURIE = new Dictionary<string, ArrayType>(
        ((ArrayType[])Enum.GetValues(typeof(ArrayType))).Select((v) => new KeyValuePair<string, ArrayType>(v.CURIE(), v))
    );

    public static string Name(this ArrayType arrayType)
    {
        switch(arrayType)
        {
            case ArrayType.BinaryDataArray:
                {
                    return "binary data array";
                }
            case ArrayType.MZArray:
                {
                    return "m/z array";
                }
            case ArrayType.IntensityArray:
                {
                    return "intensity array";
                }
            case ArrayType.ChargeArray:
                {
                    return "charge array";
                }
            case ArrayType.SignalToNoiseArray:
                {
                    return "signal to noise array";
                }
            case ArrayType.TimeArray:
                {
                    return "time array";
                }
            case ArrayType.WavelengthArray:
                {
                    return "wavelength array";
                }
            case ArrayType.NonStandardDataArray:
                {
                    return "non-standard data array";
                }
            case ArrayType.FlowRateArray:
                {
                    return "flow rate array";
                }
            case ArrayType.PressureArray:
                {
                    return "pressure array";
                }
            case ArrayType.TemperatureArray:
                {
                    return "temperature array";
                }
            case ArrayType.MeanChargeArray:
                {
                    return "mean charge array";
                }
            case ArrayType.ResolutionArray:
                {
                    return "resolution array";
                }
            case ArrayType.BaselineArray:
                {
                    return "baseline array";
                }
            case ArrayType.NoiseArray:
                {
                    return "noise array";
                }
            case ArrayType.SampledNoiseMZArray:
                {
                    return "sampled noise m/z array";
                }
            case ArrayType.SampledNoiseIntensityArray:
                {
                    return "sampled noise intensity array";
                }
            case ArrayType.SampledNoiseBaselineArray:
                {
                    return "sampled noise baseline array";
                }
            case ArrayType.IonMobilityArray:
                {
                    return "ion mobility array";
                }
            case ArrayType.MassArray:
                {
                    return "mass array";
                }
            case ArrayType.ScanningQuadrupolePositionLowerBoundMZArray:
                {
                    return "scanning quadrupole position lower bound m/z array";
                }
            case ArrayType.ScanningQuadrupolePositionUpperBoundMZArray:
                {
                    return "scanning quadrupole position upper bound m/z array";
                }
            case ArrayType.MeanIonMobilityDriftTimeArray:
                {
                    return "mean ion mobility drift time array";
                }
            case ArrayType.MeanIonMobilityArray:
                {
                    return "mean ion mobility array";
                }
            case ArrayType.MeanInverseReducedIonMobilityArray:
                {
                    return "mean inverse reduced ion mobility array";
                }
            case ArrayType.RawIonMobilityArray:
                {
                    return "raw ion mobility array";
                }
            case ArrayType.RawInverseReducedIonMobilityArray:
                {
                    return "raw inverse reduced ion mobility array";
                }
            case ArrayType.RawIonMobilityDriftTimeArray:
                {
                    return "raw ion mobility drift time array";
                }
            case ArrayType.DeconvolutedIonMobilityArray:
                {
                    return "deconvoluted ion mobility array";
                }
            case ArrayType.DeconvolutedInverseReducedIonMobilityArray:
                {
                    return "deconvoluted inverse reduced ion mobility array";
                }
            case ArrayType.DeconvolutedIonMobilityDriftTimeArray:
                {
                    return "deconvoluted ion mobility drift time array";
                }
            default:
                throw new InvalidOperationException();
        }
    }

    public static string CURIE(this ArrayType arrayType)
    {
        switch (arrayType)
        {
            case ArrayType.BinaryDataArray:
                {
                    return "MS:1000513";
                }
            case ArrayType.MZArray:
                {
                    return "MS:1000514";
                }
            case ArrayType.IntensityArray:
                {
                    return "MS:1000515";
                }
            case ArrayType.ChargeArray:
                {
                    return "MS:1000516";
                }
            case ArrayType.SignalToNoiseArray:
                {
                    return "MS:1000517";
                }
            case ArrayType.TimeArray:
                {
                    return "MS:1000595";
                }
            case ArrayType.WavelengthArray:
                {
                    return "MS:1000617";
                }
            case ArrayType.NonStandardDataArray:
                {
                    return "MS:1000786";
                }
            case ArrayType.FlowRateArray:
                {
                    return "MS:1000820";
                }
            case ArrayType.PressureArray:
                {
                    return "MS:1000821";
                }
            case ArrayType.TemperatureArray:
                {
                    return "MS:1000822";
                }
            case ArrayType.MeanChargeArray:
                {
                    return "MS:1002478";
                }
            case ArrayType.ResolutionArray:
                {
                    return "MS:1002529";
                }
            case ArrayType.BaselineArray:
                {
                    return "MS:1002530";
                }
            case ArrayType.NoiseArray:
                {
                    return "MS:1002742";
                }
            case ArrayType.SampledNoiseMZArray:
                {
                    return "MS:1002743";
                }
            case ArrayType.SampledNoiseIntensityArray:
                {
                    return "MS:1002744";
                }
            case ArrayType.SampledNoiseBaselineArray:
                {
                    return "MS:1002745";
                }
            case ArrayType.IonMobilityArray:
                {
                    return "MS:1002893";
                }
            case ArrayType.MassArray:
                {
                    return "MS:1003143";
                }
            case ArrayType.ScanningQuadrupolePositionLowerBoundMZArray:
                {
                    return "MS:1003157";
                }
            case ArrayType.ScanningQuadrupolePositionUpperBoundMZArray:
                {
                    return "MS:1003158";
                }
            case ArrayType.MeanIonMobilityDriftTimeArray:
                {
                    return "MS:1002477";
                }
            case ArrayType.MeanIonMobilityArray:
                {
                    return "MS:1002816";
                }
            case ArrayType.MeanInverseReducedIonMobilityArray:
                {
                    return "MS:1003006";
                }
            case ArrayType.RawIonMobilityArray:
                {
                    return "MS:1003007";
                }
            case ArrayType.RawInverseReducedIonMobilityArray:
                {
                    return "MS:1003008";
                }
            case ArrayType.RawIonMobilityDriftTimeArray:
                {
                    return "MS:1003153";
                }
            case ArrayType.DeconvolutedIonMobilityArray:
                {
                    return "MS:1003154";
                }
            case ArrayType.DeconvolutedInverseReducedIonMobilityArray:
                {
                    return "MS:1003155";
                }
            case ArrayType.DeconvolutedIonMobilityDriftTimeArray:
                {
                    return "MS:1003156";
                }
            default:
                throw new InvalidOperationException();
        }
    }
}

public enum Unit
{
    Unit,
    MZ,
    IntensityUnit,
    EnergyUnit,
    ThS,
    VoltSecondPerSquareCentimeter,
    LengthUnit,
    MassUnit,
    TimeUnit,
    TemperatureUnit,
    BaseUnit,
    AreaUnit,
    VolumeUnit,
    FrequencyUnit,
    PressureUnit,
    AngleUnit,
    DensityUnit,
    DimensionlessUnit,
    ElectricPotentialDifferenceUnit,
    MagneticFluxDensityUnit,
    ElectricFieldStrengthUnit,
    VolumetricFlowRateUnit,
    NumberOfDetectorCounts,
    PercentOfBasePeak,
    CountsPerSecond,
    PercentOfBasePeakTimes100,
    Meter,
    Micrometer,
    Nanometer,
    Gram,
    Dalton,
    Kilodalton,
    Second,
    Millisecond,
    Minute,
    Nanosecond,
    Kelvin,
    DegreeCelsius,
    SquareAngstrom,
    Milliliter,
    Hertz,
    Pascal,
    PlaneAngleUnit,
    MassDensityUnit,
    PartsPerNotationUnit,
    CountUnit,
    Ratio,
    AbsorbanceUnit,
    Volt,
    Tesla,
    VoltPerMeter,
    MicrolitersPerMinute,
    Degree,
    GramPerLiter,
    PartsPerMillion,
    Percent,
    Fraction,
}

public static class UnitMethods
{
    public static readonly Dictionary<string, Unit> FromCURIE = new Dictionary<string, Unit>(
        ((Unit[])Enum.GetValues(typeof(Unit))).Select((v) => new KeyValuePair<string, Unit>(v.CURIE(), v))
    );

    public static string NameForColumn(this Unit unit)
    {
        var name = unit.Name();
        name = string.Join("_", name.Replace("/", "").Replace(" unit", "").Split(" "));
        return name;
    }

    public static string Name(this Unit unit)
    {
        switch(unit)
        {
            case Unit.Unit:
                {
                    return "unit";
                }
            case Unit.MZ:
                {
                    return "m/z";
                }
            case Unit.IntensityUnit:
                {
                    return "intensity unit";
                }
            case Unit.EnergyUnit:
                {
                    return "energy unit";
                }
            case Unit.ThS:
                {
                    return "Th/s";
                }
            case Unit.VoltSecondPerSquareCentimeter:
                {
                    return "volt-second per square centimeter";
                }
            case Unit.LengthUnit:
                {
                    return "length unit";
                }
            case Unit.MassUnit:
                {
                    return "mass unit";
                }
            case Unit.TimeUnit:
                {
                    return "time unit";
                }
            case Unit.TemperatureUnit:
                {
                    return "temperature unit";
                }
            case Unit.BaseUnit:
                {
                    return "base unit";
                }
            case Unit.AreaUnit:
                {
                    return "area unit";
                }
            case Unit.VolumeUnit:
                {
                    return "volume unit";
                }
            case Unit.FrequencyUnit:
                {
                    return "frequency unit";
                }
            case Unit.PressureUnit:
                {
                    return "pressure unit";
                }
            case Unit.AngleUnit:
                {
                    return "angle unit";
                }
            case Unit.DensityUnit:
                {
                    return "density unit";
                }
            case Unit.DimensionlessUnit:
                {
                    return "dimensionless unit";
                }
            case Unit.ElectricPotentialDifferenceUnit:
                {
                    return "electric potential difference unit";
                }
            case Unit.MagneticFluxDensityUnit:
                {
                    return "magnetic flux density unit";
                }
            case Unit.ElectricFieldStrengthUnit:
                {
                    return "electric field strength unit";
                }
            case Unit.VolumetricFlowRateUnit:
                {
                    return "volumetric flow rate unit";
                }
            case Unit.NumberOfDetectorCounts:
                {
                    return "number of detector counts";
                }
            case Unit.PercentOfBasePeak:
                {
                    return "percent of base peak";
                }
            case Unit.CountsPerSecond:
                {
                    return "counts per second";
                }
            case Unit.PercentOfBasePeakTimes100:
                {
                    return "percent of base peak times 100";
                }
            case Unit.Meter:
                {
                    return "meter";
                }
            case Unit.Micrometer:
                {
                    return "micrometer";
                }
            case Unit.Nanometer:
                {
                    return "nanometer";
                }
            case Unit.Gram:
                {
                    return "gram";
                }
            case Unit.Dalton:
                {
                    return "dalton";
                }
            case Unit.Kilodalton:
                {
                    return "kilodalton";
                }
            case Unit.Second:
                {
                    return "second";
                }
            case Unit.Millisecond:
                {
                    return "millisecond";
                }
            case Unit.Minute:
                {
                    return "minute";
                }
            case Unit.Nanosecond:
                {
                    return "nanosecond";
                }
            case Unit.Kelvin:
                {
                    return "kelvin";
                }
            case Unit.DegreeCelsius:
                {
                    return "degree Celsius";
                }
            case Unit.SquareAngstrom:
                {
                    return "square angstrom";
                }
            case Unit.Milliliter:
                {
                    return "milliliter";
                }
            case Unit.Hertz:
                {
                    return "hertz";
                }
            case Unit.Pascal:
                {
                    return "pascal";
                }
            case Unit.PlaneAngleUnit:
                {
                    return "plane angle unit";
                }
            case Unit.MassDensityUnit:
                {
                    return "mass density unit";
                }
            case Unit.PartsPerNotationUnit:
                {
                    return "parts per notation unit";
                }
            case Unit.CountUnit:
                {
                    return "count unit";
                }
            case Unit.Ratio:
                {
                    return "ratio";
                }
            case Unit.AbsorbanceUnit:
                {
                    return "absorbance unit";
                }
            case Unit.Volt:
                {
                    return "volt";
                }
            case Unit.Tesla:
                {
                    return "tesla";
                }
            case Unit.VoltPerMeter:
                {
                    return "volt per meter";
                }
            case Unit.MicrolitersPerMinute:
                {
                    return "microliters per minute";
                }
            case Unit.Degree:
                {
                    return "degree";
                }
            case Unit.GramPerLiter:
                {
                    return "gram per liter";
                }
            case Unit.PartsPerMillion:
                {
                    return "parts per million";
                }
            case Unit.Percent:
                {
                    return "percent";
                }
            case Unit.Fraction:
                {
                    return "fraction";
                }
        }
        throw new InvalidOperationException();
    }

    public static string CURIE(this Unit unit)
    {
        switch(unit)
        {
            case Unit.Unit:
                {
                    return "UO:0000000";
                }
            case Unit.MZ:
                {
                    return "MS:1000040";
                }
            case Unit.IntensityUnit:
                {
                    return "MS:1000043";
                }
            case Unit.EnergyUnit:
                {
                    return "MS:1000046";
                }
            case Unit.ThS:
                {
                    return "MS:1000807";
                }
            case Unit.VoltSecondPerSquareCentimeter:
                {
                    return "MS:1002814";
                }
            case Unit.LengthUnit:
                {
                    return "UO:0000001";
                }
            case Unit.MassUnit:
                {
                    return "UO:0000002";
                }
            case Unit.TimeUnit:
                {
                    return "UO:0000003";
                }
            case Unit.TemperatureUnit:
                {
                    return "UO:0000005";
                }
            case Unit.BaseUnit:
                {
                    return "UO:0000045";
                }
            case Unit.AreaUnit:
                {
                    return "UO:0000047";
                }
            case Unit.VolumeUnit:
                {
                    return "UO:0000095";
                }
            case Unit.FrequencyUnit:
                {
                    return "UO:0000105";
                }
            case Unit.PressureUnit:
                {
                    return "UO:0000109";
                }
            case Unit.AngleUnit:
                {
                    return "UO:0000121";
                }
            case Unit.DensityUnit:
                {
                    return "UO:0000182";
                }
            case Unit.DimensionlessUnit:
                {
                    return "UO:0000186";
                }
            case Unit.ElectricPotentialDifferenceUnit:
                {
                    return "UO:0000217";
                }
            case Unit.MagneticFluxDensityUnit:
                {
                    return "UO:0000227";
                }
            case Unit.ElectricFieldStrengthUnit:
                {
                    return "UO:0000267";
                }
            case Unit.VolumetricFlowRateUnit:
                {
                    return "UO:0000270";
                }
            case Unit.NumberOfDetectorCounts:
                {
                    return "MS:1000131";
                }
            case Unit.PercentOfBasePeak:
                {
                    return "MS:1000132";
                }
            case Unit.CountsPerSecond:
                {
                    return "MS:1000814";
                }
            case Unit.PercentOfBasePeakTimes100:
                {
                    return "MS:1000905";
                }
            case Unit.Meter:
                {
                    return "UO:0000008";
                }
            case Unit.Micrometer:
                {
                    return "UO:0000017";
                }
            case Unit.Nanometer:
                {
                    return "UO:0000018";
                }
            case Unit.Gram:
                {
                    return "UO:0000021";
                }
            case Unit.Dalton:
                {
                    return "UO:0000221";
                }
            case Unit.Kilodalton:
                {
                    return "UO:0000222";
                }
            case Unit.Second:
                {
                    return "UO:0000010";
                }
            case Unit.Millisecond:
                {
                    return "UO:0000028";
                }
            case Unit.Minute:
                {
                    return "UO:0000031";
                }
            case Unit.Nanosecond:
                {
                    return "UO:0000150";
                }
            case Unit.Kelvin:
                {
                    return "UO:0000012";
                }
            case Unit.DegreeCelsius:
                {
                    return "UO:0000027";
                }
            case Unit.SquareAngstrom:
                {
                    return "UO:0000324";
                }
            case Unit.Milliliter:
                {
                    return "UO:0000098";
                }
            case Unit.Hertz:
                {
                    return "UO:0000106";
                }
            case Unit.Pascal:
                {
                    return "UO:0000110";
                }
            case Unit.PlaneAngleUnit:
                {
                    return "UO:0000122";
                }
            case Unit.MassDensityUnit:
                {
                    return "UO:0000052";
                }
            case Unit.PartsPerNotationUnit:
                {
                    return "UO:0000166";
                }
            case Unit.CountUnit:
                {
                    return "UO:0000189";
                }
            case Unit.Ratio:
                {
                    return "UO:0000190";
                }
            case Unit.AbsorbanceUnit:
                {
                    return "UO:0000269";
                }
            case Unit.Volt:
                {
                    return "UO:0000218";
                }
            case Unit.Tesla:
                {
                    return "UO:0000228";
                }
            case Unit.VoltPerMeter:
                {
                    return "UO:0000268";
                }
            case Unit.MicrolitersPerMinute:
                {
                    return "UO:0000271";
                }
            case Unit.Degree:
                {
                    return "UO:0000185";
                }
            case Unit.GramPerLiter:
                {
                    return "UO:0000175";
                }
            case Unit.PartsPerMillion:
                {
                    return "UO:0000169";
                }
            case Unit.Percent:
                {
                    return "UO:0000187";
                }
            case Unit.Fraction:
                {
                    return "UO:0000191";
                }
        }
        throw new InvalidOperationException();
    }
}

public enum SpectrumProperties
{
    SpectrumProperty,
    SpectrumAttribute,
    TotalIonCurrent,
    BasePeakMZ,
    BasePeakIntensity,
    HighestObservedMZ,
    LowestObservedMZ,
    HighestObservedWavelength,
    LowestObservedWavelength,
    PeakListScans,
    PeakListRawScans,
    NumberOfPeaks,
    NumberOfDataPoints,
    LowestObservedIonMobility,
    HighestObservedIonMobility,
    MsLevel,
    SourceDataFile,
    SpectrumTitle,
    LibrarySpectrumName,
    UniversalSpectrumIdentifier,
    SpectrumAggregationAttribute,
    SpectrumOriginAttribute,
    PreviousMsn1ScanPrecursorIntensity,
    PrecursorApexIntensity,
    NistMspComment,
    IonMobilityFrameRepresentation,
    RawDataFile,
    ProcessedDataFile,
    SpectrumAggregationType,
    NumberOfReplicateSpectraAvailable,
    NumberOfReplicateSpectraUsed,
    SummaryStatisticsOfReplicates,
    NumberOfReplicatesSpectraUsedFromSource,
    SpectrumOriginType,
    IonMobilityProfileFrame,
    IonMobilityCentroidFrame,
    SingletonSpectrum,
    ConsensusSpectrum,
    BestReplicateSpectrum,
    PredictedSpectrum,
    RetentionTime,
    NormalizedRetentionTime,
    ExperimentalPrecursorMonoisotopicMZ,
    MonoisotopicMZDeviation,
    AverageMZDeviation,
    SpectralDotProductToAggregatedSpectrum,
    ObservedSpectrum,
    DemultiplexedSpectrum,
    DecoySpectrum,
    SelectedFragmentTheoreticalMZObservedIntensitySpectrum,
    LocalRetentionTime,
    PredictedRetentionTime,
    ShuffleAndRepositionDecoySpectrum,
    PrecursorShiftDecoySpectrum,
    UnnaturalPeptidoformDecoySpectrum,
    UnrelatedSpeciesDecoySpectrum,
}

public static class SpectrumPropertiesMethods
{
    public static readonly Dictionary<string, SpectrumProperties> FromCURIE = new Dictionary<string, SpectrumProperties>(
        ((SpectrumProperties[])Enum.GetValues(typeof(SpectrumProperties))).Select((v) => new KeyValuePair<string, SpectrumProperties>(v.CURIE(), v))
    );
    public static string Name(this SpectrumProperties term)
    {
        switch (term)
        {
            case SpectrumProperties.SpectrumProperty: return "spectrum property";
            case SpectrumProperties.SpectrumAttribute: return "spectrum attribute";
            case SpectrumProperties.TotalIonCurrent: return "total ion current";
            case SpectrumProperties.BasePeakMZ: return "base peak m/z";
            case SpectrumProperties.BasePeakIntensity: return "base peak intensity";
            case SpectrumProperties.HighestObservedMZ: return "highest observed m/z";
            case SpectrumProperties.LowestObservedMZ: return "lowest observed m/z";
            case SpectrumProperties.HighestObservedWavelength: return "highest observed wavelength";
            case SpectrumProperties.LowestObservedWavelength: return "lowest observed wavelength";
            case SpectrumProperties.PeakListScans: return "peak list scans";
            case SpectrumProperties.PeakListRawScans: return "peak list raw scans";
            case SpectrumProperties.NumberOfPeaks: return "number of peaks";
            case SpectrumProperties.NumberOfDataPoints: return "number of data points";
            case SpectrumProperties.LowestObservedIonMobility: return "lowest observed ion mobility";
            case SpectrumProperties.HighestObservedIonMobility: return "highest observed ion mobility";
            case SpectrumProperties.MsLevel: return "ms level";
            case SpectrumProperties.SourceDataFile: return "source data file";
            case SpectrumProperties.SpectrumTitle: return "spectrum title";
            case SpectrumProperties.LibrarySpectrumName: return "library spectrum name";
            case SpectrumProperties.UniversalSpectrumIdentifier: return "universal spectrum identifier";
            case SpectrumProperties.SpectrumAggregationAttribute: return "spectrum aggregation attribute";
            case SpectrumProperties.SpectrumOriginAttribute: return "spectrum origin attribute";
            case SpectrumProperties.PreviousMsn1ScanPrecursorIntensity: return "previous MSn-1 scan precursor intensity";
            case SpectrumProperties.PrecursorApexIntensity: return "precursor apex intensity";
            case SpectrumProperties.NistMspComment: return "NIST msp comment";
            case SpectrumProperties.IonMobilityFrameRepresentation: return "ion mobility frame representation";
            case SpectrumProperties.RawDataFile: return "raw data file";
            case SpectrumProperties.ProcessedDataFile: return "processed data file";
            case SpectrumProperties.SpectrumAggregationType: return "spectrum aggregation type";
            case SpectrumProperties.NumberOfReplicateSpectraAvailable: return "number of replicate spectra available";
            case SpectrumProperties.NumberOfReplicateSpectraUsed: return "number of replicate spectra used";
            case SpectrumProperties.SummaryStatisticsOfReplicates: return "summary statistics of replicates";
            case SpectrumProperties.NumberOfReplicatesSpectraUsedFromSource: return "number of replicates spectra used from source";
            case SpectrumProperties.SpectrumOriginType: return "spectrum origin type";
            case SpectrumProperties.IonMobilityProfileFrame: return "ion mobility profile frame";
            case SpectrumProperties.IonMobilityCentroidFrame: return "ion mobility centroid frame";
            case SpectrumProperties.SingletonSpectrum: return "singleton spectrum";
            case SpectrumProperties.ConsensusSpectrum: return "consensus spectrum";
            case SpectrumProperties.BestReplicateSpectrum: return "best replicate spectrum";
            case SpectrumProperties.PredictedSpectrum: return "predicted spectrum";
            case SpectrumProperties.RetentionTime: return "retention time";
            case SpectrumProperties.NormalizedRetentionTime: return "normalized retention time";
            case SpectrumProperties.ExperimentalPrecursorMonoisotopicMZ: return "experimental precursor monoisotopic m/z";
            case SpectrumProperties.MonoisotopicMZDeviation: return "monoisotopic m/z deviation";
            case SpectrumProperties.AverageMZDeviation: return "average m/z deviation";
            case SpectrumProperties.SpectralDotProductToAggregatedSpectrum: return "spectral dot product to aggregated spectrum";
            case SpectrumProperties.ObservedSpectrum: return "observed spectrum";
            case SpectrumProperties.DemultiplexedSpectrum: return "demultiplexed spectrum";
            case SpectrumProperties.DecoySpectrum: return "decoy spectrum";
            case SpectrumProperties.SelectedFragmentTheoreticalMZObservedIntensitySpectrum: return "selected fragment theoretical m/z observed intensity spectrum";
            case SpectrumProperties.LocalRetentionTime: return "local retention time";
            case SpectrumProperties.PredictedRetentionTime: return "predicted retention time";
            case SpectrumProperties.ShuffleAndRepositionDecoySpectrum: return "shuffle-and-reposition decoy spectrum";
            case SpectrumProperties.PrecursorShiftDecoySpectrum: return "precursor shift decoy spectrum";
            case SpectrumProperties.UnnaturalPeptidoformDecoySpectrum: return "unnatural peptidoform decoy spectrum";
            case SpectrumProperties.UnrelatedSpeciesDecoySpectrum: return "unrelated species decoy spectrum";
        }
        throw new InvalidOperationException();
    }

    public static string CURIE(this SpectrumProperties term)
    {
        switch (term)
        {
            case SpectrumProperties.SpectrumProperty: return "MS:1003058";
            case SpectrumProperties.SpectrumAttribute: return "MS:1000499";
            case SpectrumProperties.TotalIonCurrent: return "MS:1000285";
            case SpectrumProperties.BasePeakMZ: return "MS:1000504";
            case SpectrumProperties.BasePeakIntensity: return "MS:1000505";
            case SpectrumProperties.HighestObservedMZ: return "MS:1000527";
            case SpectrumProperties.LowestObservedMZ: return "MS:1000528";
            case SpectrumProperties.HighestObservedWavelength: return "MS:1000618";
            case SpectrumProperties.LowestObservedWavelength: return "MS:1000619";
            case SpectrumProperties.PeakListScans: return "MS:1000797";
            case SpectrumProperties.PeakListRawScans: return "MS:1000798";
            case SpectrumProperties.NumberOfPeaks: return "MS:1003059";
            case SpectrumProperties.NumberOfDataPoints: return "MS:1003060";
            case SpectrumProperties.LowestObservedIonMobility: return "MS:1003437";
            case SpectrumProperties.HighestObservedIonMobility: return "MS:1003438";
            case SpectrumProperties.MsLevel: return "MS:1000511";
            case SpectrumProperties.SourceDataFile: return "MS:1000577";
            case SpectrumProperties.SpectrumTitle: return "MS:1000796";
            case SpectrumProperties.LibrarySpectrumName: return "MS:1003061";
            case SpectrumProperties.UniversalSpectrumIdentifier: return "MS:1003063";
            case SpectrumProperties.SpectrumAggregationAttribute: return "MS:1003064";
            case SpectrumProperties.SpectrumOriginAttribute: return "MS:1003071";
            case SpectrumProperties.PreviousMsn1ScanPrecursorIntensity: return "MS:1003085";
            case SpectrumProperties.PrecursorApexIntensity: return "MS:1003086";
            case SpectrumProperties.NistMspComment: return "MS:1003102";
            case SpectrumProperties.IonMobilityFrameRepresentation: return "MS:1003439";
            case SpectrumProperties.RawDataFile: return "MS:1003083";
            case SpectrumProperties.ProcessedDataFile: return "MS:1003084";
            case SpectrumProperties.SpectrumAggregationType: return "MS:1003065";
            case SpectrumProperties.NumberOfReplicateSpectraAvailable: return "MS:1003069";
            case SpectrumProperties.NumberOfReplicateSpectraUsed: return "MS:1003070";
            case SpectrumProperties.SummaryStatisticsOfReplicates: return "MS:1003295";
            case SpectrumProperties.NumberOfReplicatesSpectraUsedFromSource: return "MS:1003296";
            case SpectrumProperties.SpectrumOriginType: return "MS:1003072";
            case SpectrumProperties.IonMobilityProfileFrame: return "MS:1003440";
            case SpectrumProperties.IonMobilityCentroidFrame: return "MS:1003441";
            case SpectrumProperties.SingletonSpectrum: return "MS:1003066";
            case SpectrumProperties.ConsensusSpectrum: return "MS:1003067";
            case SpectrumProperties.BestReplicateSpectrum: return "MS:1003068";
            case SpectrumProperties.PredictedSpectrum: return "MS:1003074";
            case SpectrumProperties.RetentionTime: return "MS:1000894";
            case SpectrumProperties.NormalizedRetentionTime: return "MS:1000896";
            case SpectrumProperties.ExperimentalPrecursorMonoisotopicMZ: return "MS:1003208";
            case SpectrumProperties.MonoisotopicMZDeviation: return "MS:1003209";
            case SpectrumProperties.AverageMZDeviation: return "MS:1003210";
            case SpectrumProperties.SpectralDotProductToAggregatedSpectrum: return "MS:1003324";
            case SpectrumProperties.ObservedSpectrum: return "MS:1003073";
            case SpectrumProperties.DemultiplexedSpectrum: return "MS:1003075";
            case SpectrumProperties.DecoySpectrum: return "MS:1003192";
            case SpectrumProperties.SelectedFragmentTheoreticalMZObservedIntensitySpectrum: return "MS:1003424";
            case SpectrumProperties.LocalRetentionTime: return "MS:1000895";
            case SpectrumProperties.PredictedRetentionTime: return "MS:1000897";
            case SpectrumProperties.ShuffleAndRepositionDecoySpectrum: return "MS:1003193";
            case SpectrumProperties.PrecursorShiftDecoySpectrum: return "MS:1003194";
            case SpectrumProperties.UnnaturalPeptidoformDecoySpectrum: return "MS:1003195";
            case SpectrumProperties.UnrelatedSpeciesDecoySpectrum: return "MS:1003196";
        }
        throw new InvalidOperationException();
    }
}

public enum BinaryDataType
{
    Int32,
    Int64,
    Float32,
    Float64,
    ASCII
}

public static class BinaryDataTypeMethods
{
    public static readonly Dictionary<string, BinaryDataType> FromCURIE = new Dictionary<string, BinaryDataType>(
        ((BinaryDataType[])Enum.GetValues(typeof(BinaryDataType))).Select((v) => new KeyValuePair<string, BinaryDataType>(v.CURIE(), v))
    );

    public static string NameForColumn(this BinaryDataType dataType)
    {
        switch (dataType)
        {
            case BinaryDataType.Float32:
                {
                    return "float32";
                }
            case BinaryDataType.Float64:
                {
                    return "float64";
                }
            case BinaryDataType.Int32:
                {
                    return "integer32";
                }
            case BinaryDataType.Int64:
                {
                    return "integer64";
                }
            case BinaryDataType.ASCII:
                {
                    return "ascii";
                }
            default: throw new NotImplementedException();
        }
    }

    public static string Name(this BinaryDataType dataType)
    {
        switch (dataType)
        {
            case BinaryDataType.Float32:
                {
                    return "32-bit float";
                }
            case BinaryDataType.Float64:
                {
                    return "64-bit float";
                }
            case BinaryDataType.Int32:
                {
                    return "32-bit integer";
                }
            case BinaryDataType.Int64:
                {
                    return "64-bit integer";
                }
            case BinaryDataType.ASCII:
                {
                    return "null-terminated ASCII string";
                }
            default: throw new NotImplementedException();
        }
    }

    public static string CURIE(this BinaryDataType dataType)
    {
        switch (dataType)
        {
            case BinaryDataType.Float32:
                {
                    return "MS:1000521";
                }
            case BinaryDataType.Float64:
                {
                    return "MS:1000523";
                }
            case BinaryDataType.Int32:
                {
                    return "MS:1000519";
                }
            case BinaryDataType.Int64:
                {
                    return "MS:1000522";
                }
            case BinaryDataType.ASCII:
                {
                    return "MS:1001479";
                }
            default: throw new NotImplementedException();
        }
    }
}

public record ColumnParam
{
    public string Name;
    public string? CURIE;
    public string? UnitCURIE;
    public int Index;
    public string OriginalName;
    public bool IsUnitOnly = false;

    public static ColumnParam FromFieldIndex(Field field, int index)
    {
        var tokens_ = field.Name.Split("_");
        if (tokens_.Length < 3)
        {
            return new ColumnParam(field.Name, null, null, index, field.Name);
        }
        var tokens = tokens_.ToList();
        var cvPrefix = tokens[0];
        if (cvPrefix != "MS" && cvPrefix != "UO")
        {
            return new ColumnParam(field.Name, null, null, index, field.Name);
        }
        var accession = tokens[1];
        var curie = string.Format("{0}:{1}", cvPrefix, accession);
        var indexOfUnit = tokens.FindIndex((v) => v == "unit");
        if (indexOfUnit == -1)
        {
            var name = string.Join('_', tokens.Slice(2, tokens.Count - 2));
            return new ColumnParam(name, curie, null, index, field.Name, false);
        }
        else if (indexOfUnit < tokens.Count - 1)
        {
            var name = string.Join('_', tokens.Slice(2, indexOfUnit - 2));
            var unit = string.Join(':', tokens.Slice(indexOfUnit + 1, tokens.Count - indexOfUnit - 1));
            return new ColumnParam(name, curie, unit, index, field.Name, false);
        }
        else
        {
            var name = string.Join('_', tokens.Slice(2, tokens.Count));
            return new ColumnParam(name, curie, null, index, field.Name, true);
        }
    }

    public static string Inflect(string accessionCURIE, string name, string? unit=null)
    {
        var tokens = accessionCURIE.Split(":").ToList();
        tokens.AddRange(name.Split(" ").Select((v) => v.Replace("m/z", "mz")));
        if (unit != null)
        {
            tokens.Add("unit");
            tokens.AddRange(unit.Split(":"));
        }
        return string.Join("_", tokens);
    }

    public static List<ColumnParam> FromFields(IEnumerable<Field> fields)
    {
        List<ColumnParam> cols = new();

        int i = 0;
        foreach(var f in fields)
        {
            cols.Add(FromFieldIndex(f, i));
            i += 1;
        }

        return cols;
    }

    public ColumnParam(string name, string? curie, string? unit, int index, string originalName, bool isUnitOnly=false)
    {
        Name = name;
        CURIE = curie;
        UnitCURIE = unit;
        Index = index;
        OriginalName = originalName;
        IsUnitOnly = isUnitOnly;
    }
}




[JsonConverter(typeof(ParamJsonConverter))]
public class Param
{
    public string Name { get; set; }
    public string? AccessionCURIE { get; set; }
    internal object? rawValue;
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

    public Param(string name, object? rawValue)
    {
        Name = name;
        this.rawValue = rawValue;
    }

    public Param(string name, string accession, object? rawValue)
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
            switch (propertyName)
            {
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
                            }
                            else if (reader.TryGetInt64(out vlong))
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
        }
        else if (value.IsNull())
        {
            writer.WriteNull("value");
        }

        writer.WriteString("unit", value.UnitCURIE);
        writer.WriteEndObject();
    }
}
