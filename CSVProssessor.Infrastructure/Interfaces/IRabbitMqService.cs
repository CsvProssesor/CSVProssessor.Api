namespace CSVProssessor.Infrastructure.Interfaces
{
    /// <summary>
    /// A service for publishing messages to RabbitMQ.
    /// Consumption is handled by background services.
    /// </summary>
    public interface IRabbitMqService
    {
        /// <summary>
        /// Publishes a message to a specified RabbitMQ queue or exchange (topic).
        /// </summary>
        /// <typeparam name="T">The type of the message object.</typeparam>
        /// <param name="destination">The name of the queue or exchange.</param>
        /// <param name="message">The message object to be serialized and sent.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task PublishAsync<T>(string destination, T message);
    }
}