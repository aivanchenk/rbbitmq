namespace Services;

/// <summary>
/// Wrapper for RPC calls and responses.
/// </summary>
public class RPCMessage
{
	/// <summary>
	/// Action type.
	/// </summary>
	public string Action { get; set; } = "";

	/// <summary>
	/// Action data.
	/// </summary>
	public string Data { get; set; } = "";
}