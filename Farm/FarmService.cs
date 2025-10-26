namespace Servers;

using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using NLog;
using Newtonsoft.Json;

using Services;


/// <summary>
/// Service
/// </summary>
class FarmService
{
	/// <summary>
	/// Name of the request exchange.
	/// </summary>
	private static readonly String ExchangeName = "T120B180.Farm.Exchange";

	/// <summary>
	/// Name of the request queue.
	/// </summary>
	private static readonly String ServerQueueName = "T120B180.Farm.FarmService";


	/// <summary>
	/// Logger for this class.
	/// </summary>
	private Logger log = LogManager.GetCurrentClassLogger();


	/// <summary>
	/// Connection to RabbitMQ message broker.
	/// </summary>
	private IConnection rmqConn;

	/// <summary>
	/// Communications channel to RabbitMQ message broker.
	/// </summary>
	private IModel rmqChann;

	/// <summary>
	/// Service logic.
	/// </summary>
	private FarmLogic logic = new FarmLogic();


	// /// <summary>
	// /// Constructor.
	// /// </summary>
	public FarmService()
	{
		//connect to the RabbitMQ message broker
		var rmqConnFact = new ConnectionFactory();
		rmqConn = rmqConnFact.CreateConnection();

		//get channel, configure exchanges and request queue
		rmqChann = rmqConn.CreateModel();

		rmqChann.ExchangeDeclare(exchange: ExchangeName, type: ExchangeType.Direct);
		rmqChann.QueueDeclare(queue: ServerQueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
		rmqChann.QueueBind(queue: ServerQueueName, exchange: ExchangeName, routingKey: ServerQueueName, arguments: null);

		//connect to the queue as consumer
		//XXX: see https://www.rabbitmq.com/dotnet-api-guide.html#concurrency for threading issues
		var rmqConsumer = new EventingBasicConsumer(rmqChann);
		rmqConsumer.Received += (consumer, delivery) => OnMessageReceived(((EventingBasicConsumer)consumer).Model, delivery);
		rmqChann.BasicConsume(queue: ServerQueueName, autoAck: true, consumer : rmqConsumer);
		
		log.Info("Farm Service started and listening for messages...");
	}

	/// <summary>
	/// Is invoked to process messages received.
	/// </summary>
	/// <param name="channel">Related communications channel.</param>
	/// <param name="msgIn">Message deliver data.</param>
	private void OnMessageReceived(IModel channel, BasicDeliverEventArgs msgIn)
	{
		try
		{
			//get call request
			var request =
				JsonConvert.DeserializeObject<RPCMessage>(
					Encoding.UTF8.GetString(
						msgIn.Body.ToArray()
					)
				);

			log.Info($"Received message: Action={request.Action}, Data={request.Data}");

			//set response as undefined by default
			RPCMessage response = null;

			//process the call
			switch( request.Action )
			{
				case "SubmitFood":
				{
					var args = JsonConvert.DeserializeAnonymousType(request.Data, new {amount = 0});
					log.Info($"Processing SubmitFood with amount: {args.amount}");

					response = new RPCMessage() {
						Action = "SubmitFoodResponse",
						Data = JsonConvert.SerializeObject(new SubmissionResult {
							IsAccepted = true,
							FailReason = null
						})
					};
					break;
				}

				case "SubmitWater":
				{
					var args = JsonConvert.DeserializeAnonymousType(request.Data, new {amount = 0});
					log.Info($"Processing SubmitWater with amount: {args.amount}");

					response = new RPCMessage() {
						Action = "SubmitWaterResponse",
						Data = JsonConvert.SerializeObject(new SubmissionResult {
							IsAccepted = true,
							FailReason = null
						})
					};
					break;
				}

				default:
				{
					log.Info($"Unsupported type of RPC action '{request.Action}'. Ignoring the message.");
					break;
				}
			}

			//response is defined? send reply message
			if( response != null )
			{
				//prepare metadata for outgoing message
				var msgOutProps = channel.CreateBasicProperties();
				msgOutProps.CorrelationId = msgIn.BasicProperties.CorrelationId;

				//send reply message to the client queue
				channel.BasicPublish(
					exchange : ExchangeName,
					routingKey : msgIn.BasicProperties.ReplyTo,
					basicProperties : msgOutProps,
					body: Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(response))
				);

				log.Info($"Sent response: {response.Action}");
			}
		}
		catch( Exception e )
		{
			log.Error(e, "Unhandled exception caught when processing a message. The message is now lost.");
		}
	}	
}