namespace Services;

/// <summary>
/// Descriptor of submission result
/// </summary>
public class SubmissionResult
{
    /// <summary>
    /// Indicates if submission attempt has accepted.
    /// </summary>
    public bool IsAccepted { get; set; }

    /// <summary>
    /// If pass submission has failed, indicates fail reason.
    /// </summary>
    public string? FailReason { get; set; }
}


public interface IFarmService
{
    /// <summary>
	/// Try submitting Food resource.
	/// </summary>
	/// <param name="amount">Amount of food submitted.</param>
	/// <returns>Submit result descriptor.</returns>
    SubmissionResult SubmitFood(int amount);
    
    /// <summary>
    /// Try submitting Water resource.
    /// </summary>
    /// <param name="amount">Amount of water submitted.</param>
    /// <returns>Submit result descriptor.</returns>
    SubmissionResult SubmitWater(int amount);
}