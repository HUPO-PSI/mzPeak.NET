namespace MZPeak.Compute;

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

using MathNet.Numerics.LinearAlgebra;
using Microsoft.Extensions.Logging;
using MZPeak.ControlledVocabulary;


/// <summary>
/// Generic API for coordinate grids
/// </summary>
public interface GridLike
{

    /// <summary>
    /// The controlled vocabulary accession number for the grid model or category
    /// </summary>
    public string ModelAccession();

    /// <summary>
    /// Convert from a grid index to a physical coordinate
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public double FromIndex(uint index);
    /// <summary>
    /// Convert from a physical coordinate to a grid index
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public double ToIndex(double value);

    /// <summary>
    /// Translate the model object into a list of coefficients that along with the accession can be used to
    /// reconstruct the grid model exactly.
    /// </summary>
    /// <returns></returns>
    public List<double> Parameters();

    /// <summary>
    /// An alternative constructor that checks if the accession matches the static Accession and has the
    /// correct number of parameters (in the order provided by the Parameters method) to create the grid model
    /// or throws an InvalidDataException otherwise.
    /// </summary>
    /// <param name="accession"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public abstract static GridLike FromParameters(string accession, List<double> parameters);

    /// <summary>
    /// Compute the maximum absolute round-trip error (value -&gt; index -&gt; value) over a list of coordinates.
    /// </summary>
    /// <param name="values"></param>
    /// <returns></returns>
    public double MaxError(IReadOnlyList<double> values)
    {
        double maxErr = 0.0;
        foreach (var value in values)
        {
            double yhat = FromIndex((uint)ToIndex(value));
            double err = Math.Abs(value - yhat);
            if (err > maxErr) maxErr = err;
        }
        return maxErr;
    }
}


/// <summary>
/// Shared helper for fitting grid models with ordinary least squares simple linear regression.
/// </summary>
internal static class GridFitting
{
    /// <summary>
    /// Fit y = intercept + slope * x by ordinary least squares.
    /// </summary>
    public static (double Intercept, double Slope) SimpleLinearRegression(IReadOnlyList<double> x, IReadOnlyList<double> y)
    {
        int n = x.Count;
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
        for (int i = 0; i < n; i++)
        {
            sumX += x[i];
            sumY += y[i];
            sumXY += x[i] * y[i];
            sumXX += x[i] * x[i];
        }

        double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double intercept = (sumY - slope * sumX) / n;
        return (intercept, slope);
    }
}


public class LinearGrid : GridLike
{
    public double Intercept;
    public double Slope;
    public double Scale;

    /// <summary>
    ///  The human-readable name of the grid model or category
    /// </summary>
    public const string Name = "linear grid interpolation";
    public const string Accession = "MS:1003824";

    public string ModelAccession()
    {
        return Accession;
    }

    public LinearGrid(double intercept, double slope, double scale = 1.0)
    {
        Intercept = intercept;
        Slope = slope;
        Scale = scale;
    }

    public static GridLike FromParameters(string accession, List<double> parameters)
    {
        if (Accession != accession || parameters.Count < 2 || parameters.Count > 3)
            throw new InvalidDataException($"{accession} with {parameters.Count} parameters did not match {Accession}|{Name}");

        return new LinearGrid(parameters[0], parameters[1], parameters.Count == 3 ? parameters[2] : 1.0);
    }

    /// <summary>
    /// Fit a linear grid to a set of observed coordinates spanning [low, high], assuming the
    /// coordinates are evenly spaced across the full range of a uint32 index.
    /// </summary>
    public static LinearGrid Fit(IReadOnlyList<double> values, double low, double high, double scale = 1.0)
    {
        const double slots = uint.MaxValue;
        double stepSize = (high * scale - low * scale) / slots;
        double lowScaled = low * scale;

        var scaledValues = new double[values.Count];
        var indices = new double[values.Count];
        for (int i = 0; i < values.Count; i++)
        {
            double v = values[i] * scale;
            scaledValues[i] = v;
            indices[i] = (uint)((v - lowScaled) / stepSize);
        }

        var (intercept, slope) = GridFitting.SimpleLinearRegression(indices, scaledValues);
        return new LinearGrid(intercept, slope, scale);
    }

    public double FromIndex(uint index)
    {
        return (Intercept + index * Slope) / Scale;
    }

    public List<double> Parameters()
    {
        return [Intercept, Slope, Scale];
    }

    public double ToIndex(double value)
    {
        double index = (value * Scale - Intercept) / Slope;
        return Math.Floor(index + 0.5);
    }
}


public class SquareRootLinearGrid : GridLike
{
    public double Intercept;
    public double Slope;
    public double Scale;

    public const string Name = "square root grid interpolation";
    public const string Accession = "MS:1003825";

    public string ModelAccession()
    {
        return Accession;
    }

    public SquareRootLinearGrid(double intercept, double slope, double scale = 1.0)
    {
        Intercept = intercept;
        Slope = slope;
        Scale = scale;
    }

    public static GridLike FromParameters(string accession, List<double> parameters)
    {
        if (Accession != accession || parameters.Count < 2 || parameters.Count > 3)
            throw new InvalidDataException($"{accession} with {parameters.Count} parameters did not match {Accession}|{Name}");

        return new SquareRootLinearGrid(parameters[0], parameters[1], parameters.Count == 3 ? parameters[2] : 1.0);
    }

    /// <summary>
    /// Fit a square-root grid to a set of observed coordinates spanning [low, high], assuming the
    /// square roots of the coordinates are evenly spaced across the full range of a uint32 index.
    /// </summary>
    public static SquareRootLinearGrid Fit(IReadOnlyList<double> values, double low, double high, double scale = 1.0)
    {
        const double slots = uint.MaxValue;
        double stepSize = (high * scale - low * scale) / slots;
        double sqrtLowScaled = Math.Sqrt(low * scale);

        var sqrtValues = new double[values.Count];
        var indices = new double[values.Count];
        for (int i = 0; i < values.Count; i++)
        {
            double v = Math.Sqrt(values[i] * scale);
            sqrtValues[i] = v;
            indices[i] = (uint)((v - sqrtLowScaled) / stepSize);
        }

        var (intercept, slope) = GridFitting.SimpleLinearRegression(indices, sqrtValues);
        return new SquareRootLinearGrid(intercept, slope, scale);
    }

    public double FromIndex(uint index)
    {
        double root = Intercept + index * Slope;
        return root * root / Scale;
    }

    public List<double> Parameters()
    {
        return [Intercept, Slope, Scale];
    }

    public double ToIndex(double value)
    {
        double index = (Math.Sqrt(value * Scale) - Intercept) / Slope;
        return Math.Floor(index + 0.5);
    }
}


/// <summary>
/// Bruker timsTOF ion mobility (TIMS) calibration model, version 2.
/// </summary>
public class BrukerTimsTOFTimsLinearGrid2 : GridLike
{
    public double C6;
    public double C7;
    public double Intercept;
    public double Slope;

    public const string Name = "Bruker timsTOF TIMS calibration model v2";
    public const string Accession = "MS:9999001";

    public string ModelAccession()
    {
        return Accession;
    }

    public BrukerTimsTOFTimsLinearGrid2(double c6, double c7, double intercept, double slope)
    {
        C6 = c6;
        C7 = c7;
        Intercept = intercept;
        Slope = slope;
    }

    public static GridLike FromParameters(string accession, List<double> parameters)
    {
        if (Accession != accession || parameters.Count != 4)
            throw new InvalidDataException($"{accession} with {parameters.Count} parameters did not match {Accession}|{Name}");

        return new BrukerTimsTOFTimsLinearGrid2(parameters[0], parameters[1], parameters[2], parameters[3]);
    }

    public double FromIndex(uint index)
    {
        return 1.0 / (C6 + C7 / (Intercept + Slope * index));
    }

    public double ToIndex(double value)
    {
        double d = (1.0 / value) - C6;
        double index = ((C7 / d) - Intercept) / Slope;
        return Math.Floor(index + 0.5);
    }

    public List<double> Parameters()
    {
        return [C6, C7, Intercept, Slope];
    }
}


/// <summary>
/// Bruker timsTOF m/z calibration model, version 2ish. Converts between TOF bin index and m/z
/// via a digitizer-domain linear map composed with a (possibly non-linear) TOF-to-sqrt(m/z)
/// polynomial.
/// </summary>
public class BrukerTimsTOFMzGrid2 : GridLike
{
    public double C0;
    public double Beta;
    public double C2;
    public double C3;
    public double C4;
    public double Slope;
    public double Intercept;

    public const string Name = "Bruker timsTOF m/z calibration model v2";
    public const string Accession = "MS:9999002";

    public string ModelAccession()
    {
        return Accession;
    }

    public BrukerTimsTOFMzGrid2(double c0, double beta, double c2, double c3, double c4, double slope, double intercept)
    {
        C0 = c0;
        Beta = beta;
        C2 = c2;
        C3 = c3;
        C4 = c4;
        Slope = slope;
        Intercept = intercept;
    }

    public static GridLike FromParameters(string accession, List<double> parameters)
    {
        if (Accession != accession || parameters.Count != 7)
            throw new InvalidDataException($"{accession} with {parameters.Count} parameters did not match {Accession}|{Name}");

        return new BrukerTimsTOFMzGrid2(parameters[0], parameters[1], parameters[2], parameters[3], parameters[4], parameters[5], parameters[6]);
    }

    public double FromIndex(uint index)
    {
        double tof = index * Slope + Intercept;
        double s = (tof - C0) / Beta;

        if (C3 != 0)
        {
            // Newton-Raphson root-finding for the cubic c0 + beta*s + c2*s^2 + c3*s^3 = tof
            for (int iter = 0; iter < 8; iter++)
            {
                double f = C0 + Beta * s + C2 * s * s + C3 * s * s * s - tof;
                double df = Beta + 2.0 * C2 * s + 3.0 * C3 * s * s;
                if (df == 0) break;
                s -= f / df;
            }
        }
        else if (C2 != 0)
        {
            // Numerically stable quadratic formula for c0 + beta*s + c2*s^2 = tof
            double disc = Beta * Beta - 4.0 * C2 * (C0 - tof);
            if (disc >= 0)
            {
                double q = -0.5 * (Beta + Math.Sqrt(disc));
                s = (C0 - tof) / q;
            }
        }

        return s * s - C4;
    }

    public double ToIndex(double value)
    {
        double arg = value + C4;
        double lin = arg > 0 ? Math.Sqrt(arg) : 0.0;
        double tof = C0 + Beta * lin + C2 * lin * lin + C3 * lin * lin * lin;
        double index = (tof - Intercept) / Slope;
        return Math.Floor(index + 0.5);
    }

    public List<double> Parameters()
    {
        return [C0, Beta, C2, C3, C4, Slope, Intercept];
    }
}


public class GridPolicy
{
    public ArrayType ArrayType {get; set;}
    public GridLike? CurrentGrid {get; set;} = null;
    public bool DefaultQuadratic {get; set;} = false;
    public double? ErrorTolerance { get; set; } = null;

    public GridPolicy(ArrayType arrayType, bool quadratic = false, double? errorTolerance = null)
    {
        ArrayType = arrayType;
        DefaultQuadratic = quadratic;
        ErrorTolerance = errorTolerance;
    }

    public GridLike? FindGridFromParameters(IReadOnlyList<Param> parameters)
    {
        foreach(var p in parameters)
        {
            if (p.AccessionCURIE != null && GridModel.IsGridModel(p.AccessionCURIE))
            {
                return GridModel.FromParameters(p.AccessionCURIE, p.AsListDouble());
            }
        }
        return null;
    }

    public GridLike? Fit(List<double> values, double low, double high, double scale = 1.0)
    {
        GridLike? model;
        if (DefaultQuadratic)
        {
            model = SquareRootLinearGrid.Fit(values, low, high, scale);
        }
        else
        {
            model = LinearGrid.Fit(values, low, high, scale);
        }
        if (ErrorTolerance != null)
        {
            if (model.MaxError(values) > ErrorTolerance) model = null;
        }
        return model;
    }
}


public static class GridModel
{
    public static bool IsGridModel(string accession)
    {
        switch (accession)
        {
            case LinearGrid.Accession:
                {
                    return true;
                }
            case SquareRootLinearGrid.Accession:
                {
                    return true;
                }
            case BrukerTimsTOFTimsLinearGrid2.Accession:
                {
                    return true;
                }
            case BrukerTimsTOFMzGrid2.Accession:
                {
                    return true;
                }
            default:
                return false;
        }
    }

    public static GridLike FromParameters(string accession, List<double> parameters)
    {
        switch (accession)
        {
            case LinearGrid.Accession:
                {
                    return LinearGrid.FromParameters(accession, parameters);
                }
            case SquareRootLinearGrid.Accession:
                {
                    return SquareRootLinearGrid.FromParameters(accession, parameters);
                }
            case BrukerTimsTOFTimsLinearGrid2.Accession:
                {
                    return BrukerTimsTOFTimsLinearGrid2.FromParameters(accession, parameters);
                }
            case BrukerTimsTOFMzGrid2.Accession:
                {
                    return BrukerTimsTOFMzGrid2.FromParameters(accession, parameters);
                }
            default:
                throw new KeyNotFoundException(accession);
        }
    }
}