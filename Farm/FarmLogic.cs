namespace Servers;

using NLog;

/// <summary>
/// Describes the outcome of a resource submission attempt.
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
    public string FailReason { get; set; }
}

/// <summary>
/// Tracks the mutable state of the farm resource management system.
/// </summary>
public class FarmState
{
    /// <summary>
    /// Access lock.
    /// </summary>
    public readonly object AccessLock = new object();

    /// <summary>
    /// Total accumulated food.
    /// </summary>
    public double AccumulatedFood = 0;

    /// <summary>
    /// Total accumulated water.
    /// </summary>
    public double AccumulatedWater = 0;

    /// <summary>
    /// Farm size coefficient used for consumption calculations.
    /// </summary>
    public double farmSize = 0.0;

    /// <summary>
    /// Consumption coefficient used to reduce accumulated resources over time.
    /// </summary>
    public double consumptionCoef = 0.01;

    /// <summary>
    /// Total amount of resources consumed by the farm.
    /// </summary>
    public double totalConsumedResources = 0.0;

    /// <summary>
    /// Number of consecutive rounds the farm has been without water.
    /// </summary>
    public int thirstRounds = 0;

    /// <summary>
    /// Number of consecutive rounds the farm has been without food.
    /// </summary>
    public int starveRounds = 0;

    /// <summary>
    /// Timestamp of the last consumption event.
    /// </summary>
    public DateTime? LastConsumptionTimestamp;

    /// <summary>
    /// Indicates whether the farm is currently selling resources instead of accepting them.
    /// </summary>
    public bool IsSelling = false;

    /// <summary>
    /// Timestamp until which the farm remains in selling mode.
    /// </summary>
    public DateTime? SellingUntil = null;
}

/// <summary>
/// Encapsulates the business logic used by the farm service.
/// </summary>
public class FarmLogic
{
    /// <summary>
    /// Logger for this class.
    /// </summary>
    private Logger mLog = LogManager.GetCurrentClassLogger();

    /// <summary>
    /// Background task thread.
    /// </summary>
    private Thread mBgTaskThread;

    /// <summary>
    /// State descriptor.
    /// </summary>
    private FarmState mState = new FarmState();

    /// <summary>
    /// Provides random number generation for simulation or variability in farm-related calculations.
    /// </summary>
    private readonly Random mRandom = new Random();

    /// <summary>
    /// The base production rate or starting efficiency of the farm.
    /// </summary>
    public double baseRate = 0.05;

    /// <summary>
    /// The rate at which production grows relative to farm size.
    /// </summary>
    public double growthRate = 0.1;

    /// <summary>
    /// The maximum number of consecutive failed production rounds allowed before reset or penalty.
    /// </summary>
    public int maxFailRounds = 2;

    /// <summary>
    /// The maximum allowed size multiplier for the farm before reaching its production cap.
    /// </summary>
    public double maxFarmSize = 1.5;


    public FarmLogic()
    {
        //start the background task
        mBgTaskThread = new Thread(BackgroundTask);
        mBgTaskThread.Start();
    }

    /// <summary>
    /// Accepts a food submission and updates the farm state accordingly.
    /// </summary>
    /// <param name="amount">The amount of food being submitted.</param>
    /// <returns>The result of the submission attempt.</returns>
    public SubmissionResult SubmitFood(double amount)
    {
        lock (mState.AccessLock)
        {
            if (mState.IsSelling)
            {
                return new SubmissionResult { IsAccepted = false, FailReason = "FarmSelling" };
            }
            mState.AccumulatedFood += amount;
            return new SubmissionResult { IsAccepted = true, FailReason = string.Empty };
        }
    }

    /// <summary>
    /// Accepts a water submission and updates the farm state accordingly.
    /// </summary>
    /// <param name="amount">The amount of water being submitted.</param>
    /// <returns>The result of the submission attempt.</returns>
    public SubmissionResult SubmitWater(double amount)
    {
        lock (mState.AccessLock)
        {
            if (mState.IsSelling)
            {
                return new SubmissionResult { IsAccepted = false, FailReason = "FarmSelling" };
            }

            mState.AccumulatedWater += amount;

            return new SubmissionResult { IsAccepted = true, FailReason = string.Empty };
        }
    }

    /// <summary>
    /// Calculates a random amount of food consumed based on the farm's consumption coefficient.
    /// Handles starvation tracking and farm failure if insufficient food is available.
    /// </summary>
    /// <returns>The amount of food successfully consumed. Returns 0 if consumption fails.</returns>
    private double GetRandomFoodConsumption()
    {
        var consumption = mRandom.Next(0, 100) * mState.consumptionCoef;

        if (consumption >= mState.AccumulatedFood)
        {
            mState.starveRounds++;
            mLog.Warn($"Was unable to consume {consumption} of food.");

            if (mState.starveRounds >= maxFailRounds)
            {
                HandleFarmFailure("food");
            }

            return 0;
        }

        mLog.Info($"Consumed {consumption:F1} of food. Food left: {mState.AccumulatedFood - consumption}");
        mState.starveRounds = 0;
        return consumption;
    }

    /// <summary>
    /// Calculates a random amount of water consumed based on the farm's consumption coefficient.
    /// Handles thirst tracking and farm failure if insufficient water is available.
    /// </summary>
    /// <returns>The amount of water successfully consumed. Returns 0 if consumption fails.</returns>
    private double GetRandomWaterConsumption()
    {
        var consumption = mRandom.Next(0, 100) * mState.consumptionCoef;

        if (consumption >= mState.AccumulatedWater)
        {
            mState.thirstRounds++;
            mLog.Warn($"Was unable to consume {consumption} of water.");

            if (mState.thirstRounds >= maxFailRounds)
            {
                HandleFarmFailure("water");
            }

            return 0;
        }

        mLog.Info($"Consumed {consumption:F1} of water. Water left: {mState.AccumulatedWater - consumption}.");
        mState.thirstRounds = 0;
        return consumption;
    }

    /// <summary>
    /// Computes the current consumption coefficient based on the farm's size.
    /// The value is clamped between the base rate and 2.0 to prevent excessive growth.
    /// </summary>
    /// <returns>The computed consumption coefficient.</returns>
    private double ComputeConsumptionCoefficient()
    {
        return Math.Clamp(baseRate + growthRate * mState.farmSize, baseRate, 2.0);
    }

    /// <summary>
    /// Computes the farm size using a logarithmic scale based on total accumulated resources.
    /// </summary>
    /// <param name="total">The total combined value of farm resources.</param>
    /// <returns>The computed farm size.</returns>
    private double ComputeFarmSize(double total)
    {
        return Math.Log10(total + 1);
    }

    /// <summary>
    /// Resets all farm-related state values to their initial defaults.
    /// Used when the farm fails, is sold, or needs a full reset.
    /// </summary>
    private void ResetFarmState()
    {
        mState.AccumulatedFood = 0;
        mState.AccumulatedWater = 0;
        mState.totalConsumedResources = 0;
        mState.starveRounds = 0;
        mState.thirstRounds = 0;
        mState.farmSize = 0.0;
        mState.consumptionCoef = 0.01;
        mState.LastConsumptionTimestamp = null;
    }

    /// <summary>
    /// Handles farm failure due to repeated lack of a specific resource.
    /// Logs the failure and resets the farm state.
    /// </summary>
    /// <param name="failedResource">The resource that caused the failure (e.g., "food" or "water").</param>
    private void HandleFarmFailure(string failedResource)
    {
        mLog.Warn($"Farm has been without {failedResource} for {maxFailRounds} consecutive rounds. Farm has failed.");
        ResetFarmState();
        mLog.Info("Farm state has been reset. Background processing will continue with a fresh farm.");
    }

    /// <summary>
    /// Handles the farm selling process.
    /// Resets the farm state when the selling period has expired.
    /// </summary>
    private void HandleFarmSelling()
    {
        if (mState.IsSelling && mState.SellingUntil.HasValue && DateTime.UtcNow >= mState.SellingUntil.Value)
        {
            ResetFarmState();
            mLog.Info("Farm selling period has ended. Farm is no longer selling.");
            mState.IsSelling = false;
            mState.SellingUntil = null;
        }
    }


    /// <summary>
    /// Background task for the farm.
    /// </summary>
    public void BackgroundTask()
    {
        //intialize random number generator
        var rnd = new Random();

        //
        while (true)
        {
            Thread.Sleep(2000);

            double consumedFood = 0;
            double consumedWater = 0;

            lock (mState.AccessLock)
            {
                if (!mState.IsSelling)
                {
                    consumedFood = GetRandomFoodConsumption();
                    consumedWater = GetRandomWaterConsumption();

                    mState.AccumulatedFood -= consumedFood;
                    mState.AccumulatedWater -= consumedWater;
                    mState.LastConsumptionTimestamp = DateTime.UtcNow;

                    mState.totalConsumedResources += consumedFood + consumedWater;

                    mState.farmSize = ComputeFarmSize(mState.totalConsumedResources);
                    mState.consumptionCoef = ComputeConsumptionCoefficient();

                    mLog.Info($"Farm size after consumption {mState.farmSize:F1}, consumption coefficient has been updated to {mState.consumptionCoef:F1}.");

                    if (mState.farmSize >= maxFarmSize && !mState.IsSelling)
                    {
                        mState.IsSelling = true;
                        mState.SellingUntil = DateTime.UtcNow.AddSeconds(5); //selling period lasts for 5 seconds
                        mLog.Info($"Farm has reached maximum size of {maxFarmSize}. Farm is now selling for the next 5 seconds.");
                    }
                }
                else
                {
                    mLog.Info($"Farm is currently selling. Selling will end at {mState.SellingUntil}.");
                }

                HandleFarmSelling();
            }
        }
    }
}