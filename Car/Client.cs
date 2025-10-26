namespace Clients;

using NLog;

using Services;


/// <summary>
/// Client example.
/// </summary>
class Client
{
	/// <summary>
	/// A set of names to choose from.
	/// </summary>
	private readonly List<string> NAMES = 
		new List<string> { 
			"John", "Peter", "Jack", "Steve"
		};

	/// <summary>
	/// A set of surnames to choose from.
	/// </summary>
	private readonly List<string> SURNAMES = 
		new List<String> { 
			"Johnson", "Peterson", "Jackson", "Steveson" 
		};


	/// <summary>
	/// Logger for this class.
	/// </summary>
	Logger mLog = LogManager.GetCurrentClassLogger();

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
	private void Run() {
		//configure logging
		ConfigureLogging();

		//initialize random number generator
		var rnd = new Random();

		//run everythin in a loop to recover from connection errors
		while( true )
		{
			try {
				//connect to the server, get service client proxy
				var trafficLight = new TrafficLightClient();

				//initialize car descriptor
				var car = new CarDesc();

				car.CarNumber = 
					Enumerable.Range(1, 3)
						.Select(it => 'A' + rnd.Next('Z'-'A'))
						.Aggregate("", (res, val) => res + (char)val) +
					" " +
					Enumerable.Range(1, 3)
						.Select(it => '0' + rnd.Next('9' - '0'))
						.Aggregate("", (res, val) => res + (char)val);

				car.DriverNameSurname =
					NAMES[rnd.Next(NAMES.Count)] + 
					" " +
					SURNAMES[rnd.Next(SURNAMES.Count)];

				//get unique client id
				car.CarId = trafficLight.GetUniqueId();

				//log identity data
				mLog.Info($"I am car {car.CarId}, RegNr. {car.CarNumber}, Driver {car.DriverNameSurname}.");
				Console.Title = $"I am car {car.CarId}, RegNr. {car.CarNumber}, Driver {car.DriverNameSurname}.";
					
				//do the car stuff
				while( true )
				{
					//car state for this iteration
					var isCrashed = false;
					var isPassed = false;

					//we are driving on the road
					mLog.Info("I am driving on the road.");
					Thread.Sleep(500 + rnd.Next(1500));

					//and we see a traffic light
					mLog.Info("I see a traffic light.");

					//try passing the traffic light
					while( !isCrashed && !isPassed )
					{
						//read the light state
						var lightState = trafficLight.GetLightState();

						//give some time for light to possibly switch, before taking action
						Thread.Sleep(rnd.Next(500)); 

						//green? try passing without waiting
						if( lightState == LightState.Green )
						{
							//try passing 
							mLog.Info("Light is green, trying to pass.");							
							var par = trafficLight.Pass(car);

							//handle result
							if( par.IsSuccess )
							{
								mLog.Info("Passed, life is good.");		
								isPassed = true;					
							}
							else
							{
								mLog.Info($"Crashed because '{par.CrashReason}'.");
								isCrashed = true;
							}
						}
						//red, queue until light is green and we can pass
						else
						{
							//try entering a queue
							mLog.Info("Light is red, trying to queue.");							
							var inQueue = trafficLight.Queue(car);

							//success? wait for light and queue
							if( inQueue )
							{
								mLog.Info("I'm in queue now. Waiting for light.");

								while( !isCrashed && !isPassed )
								{
									//determine state of light and queue
									lightState = trafficLight.GetLightState();
									var firstInLine = trafficLight.IsFirstInLine(car.CarId);

									//give some time for light to possibly switch, before taking action
									Thread.Sleep(rnd.Next(500)); 

									//can pass? try it
									if( lightState == LightState.Green && firstInLine )
									{
										//try passing
										mLog.Info("Light is green and I an ready, trying to pass");
										var par = trafficLight.Pass(car);

										//handle the result
										if( par.IsSuccess )
										{
											mLog.Info("Passed, life is good.");	
											isPassed = true;							
										}
										else
										{
											mLog.Info($"Crashed because '{par.CrashReason}'.");
											isCrashed = true;
										}
									}
									//no passing yet, wait
									else
									{
										mLog.Info("Waiting some more.");
										Thread.Sleep(500 + rnd.Next(1500));
									}
								}
							}
							//could not queue (maybe light has changed)
							else
							{
								mLog.Info("Queuing failed. Will check the light again.");
							}
						}
					}

					//managed to crash? reflect on it
					if( isCrashed )
					{
						mLog.Info("Meditating on my mistakes...");
						Thread.Sleep(500 + rnd.Next(1500));
						mLog.Info("It is a new day and a new car.");
					}
				}				
			}
			catch( Exception e )
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
