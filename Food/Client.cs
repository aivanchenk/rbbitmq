namespace Clients;

using NLog;

using Services;

/// <summary>
/// Food client.
/// </summary>
class Client
{
	/// <summary>
	/// Logger for this class.
	/// </summary>
	Logger mLog = LogManager.GetCurrentClassLogger();

	/// <summary>
	/// Amount of food currently pending to be processed or delivered.
	/// </summary>
	private double pendingFood = 0.0;

	/// <summary>
	/// Configures logging subsystem.
	/// </summary>
	private void ConfigureLogging()
	{
		var config = new NLog.Config.LoggingConfiguration();

		var console =
			new NLog.Targets.ConsoleTarget("console")
			{
				Layout = @"${date:format=HH\:mm\:ss}|${level}| ${message} ${exception}"
			};
		config.AddTarget(console);
		config.AddRuleForAllLevels(console);

		LogManager.Configuration = config;
	}

	/// <summary>
	/// Program body.
	/// </summary>
	private void Run()
	{
		//configure logging
		ConfigureLogging();

		//initialize random number generator
		var rnd = new Random();

		//run everythin in a loop to recover from connection errors
		while (true)
		{
			try
			{
				//connect to the server via RabbitMQ, get service proxy
				var farmClient = new FarmClient();

				while (true)
				{
					// produce between -1.0 and 5.0 food
					double producedFood = Math.Round(rnd.NextDouble() * 6.0 - 1.0, 1);
					pendingFood += producedFood;

					var result = farmClient.SubmitFood((int)Math.Round(pendingFood));

					if (result.IsAccepted)
					{
						mLog.Info($"Submitted {pendingFood} food.");
						pendingFood = 0;
					}
					else if (result.FailReason == "FarmSelling")
					{
						mLog.Info("Farm is selling; will retry with accumulated food.");
					}
					else
					{
						mLog.Warn($"Submission failed: {result.FailReason}. Keeping {pendingFood} to retry.");
					}

					// wait between sending attempts
					Thread.Sleep(1000);
				}
			}
			catch (Exception e)
			{
				//log whatever exception to console
				mLog.Warn(e, "Unhandled exception caught. Will restart main loop.");

				//prevent console spamming
				Thread.Sleep(2000);
			}
		}
	}

	/// <summary>
	/// Program entry point.
	/// </summary>
	/// <param name="args">Command line arguments.</param>
	static void Main(string[] args)
	{
		var self = new Client();
		self.Run();
	}
}