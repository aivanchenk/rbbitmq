namespace Services;


/// <summary>
/// Car descriptor.
/// </summary>
public class CarDesc
{
	/// <summary>
	/// Car ID.
	/// </summary>
	public int CarId { get; set; }

	/// <summary>
	/// Car number.
	/// </summary>
	public string CarNumber { get; set; }

	/// <summary>
	/// Driver name and surname.
	/// </summary>
	public string DriverNameSurname { get; set; }
}


/// <summary>
/// Descriptor of pass atempt result
/// </summary>
public class PassAttemptResult
{
	/// <summary>
	/// Indicates if pass attempt has succeeded.
	/// </summary>
	public bool IsSuccess { get; set; }

	/// <summary>
	/// If pass attempt has failed, indicates crash reason.
	/// </summary>
	public string CrashReason { get; set; }
}


/// <summary>
/// Light state.
/// </summary>
public enum LightState : int
{
	Red,
	Green
}


/// <summary>
/// Service contract.
/// </summary>
public interface ITrafficLightService
{
	/// <summary>
	/// Get next unique ID from the server. Is used by cars to acquire client ID's.
	/// </summary>
	/// <returns>Unique ID.</returns>
	int GetUniqueId();

	/// <summary>
	/// Get current light state.
	/// </summary>
	/// <returns>Current light state.</returns>				
	LightState GetLightState();

	/// <summary>
	/// Queue give car at the light. Will only succeed if light is red.
	/// </summary>
	/// <param name="car">Car to queue.</param>
	/// <returns>True on success, false on failure.</returns>
	bool Queue(CarDesc car);

	/// <summary>
	/// Tell if car is first in line in queue.
	/// </summary>
	/// <param name="carId">ID of the car to check for.</param>
	/// <returns>True if car is first in line. False if not first in line or not in queue.</returns>
	bool IsFirstInLine(int carId);

	/// <summary>
	/// Try passing the traffic light. If car is in queue, it will be removed from it.
	/// </summary>
	/// <param name="car">Car descriptor.</param>
	/// <returns>Pass result descriptor.</returns>
	PassAttemptResult Pass(CarDesc car);
}
