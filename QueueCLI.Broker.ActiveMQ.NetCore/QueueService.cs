using Apache.NMS;
using Apache.NMS.ActiveMQ;
using QueueCLI.Broker.Abstractions;
using QueueCLI.Broker.ActiveMQ.NetCore.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueueCLI.Broker.ActiveMQ.NetCore
{
    public class QueueService : IQueueService
    {
        private readonly ActiveMQOptions options;
        private ISession senderSession;
        private ISession receiverSession;
        private IConnection connection;

        public QueueService(ActiveMQOptions options)
        {
            this.options = options;

            Task.Run(async () =>
            {
                var protocol = "amqp";
                if (this.options.EnableSsl)
                    protocol = "amqps";

                var address = $"{protocol}://{this.options.Username}:{this.options.Password}@{this.options.Host}:{this.options.Port}";
                var connectionFactory = new ConnectionFactory(address);
                //connectionFactory.SSL.RemoteCertificateValidationCallback
                //    += (object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors) => true;

                connection = connectionFactory.CreateConnection();
                senderSession = connection.CreateSession();
                receiverSession = connection.CreateSession();

            }).Wait();

        }

        public void Acknowledge(QueueMessage message)
        {
            throw new NotImplementedException();
        }

        public void Bind(string queue, string[] exchanges, string[] routeKeys)
        {
            throw new NotImplementedException();
        }

        public void Clean(string name)
        {
            throw new NotImplementedException();
        }

        public void Create(string queue, bool durable, bool exclusive, bool autoDelete, CreateQueueSettings settings)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public ICollection<QueueMessage> Get(string queueName, QueueServiceGetOptions options = null)
        {
            var list = new List<QueueMessage>();

            IDestination dest = receiverSession.GetDestination("example");
            IMessageConsumer consumer = receiverSession.CreateConsumer(dest);
            while (true)
            {
                var message = consumer.Receive(new TimeSpan(0, 0, 1));
                if (message != null)
                    list.Add(new QueueMessage(message.Properties["DeliveryTag"], message.Properties["Body"].ToString()));
                else
                    break;

                if (options != null && options.AutoAcknowlodge)
                    message.Acknowledge();
            }
            return list;
        }

        public void Publish(string message, QueueServicePublishingOptions options = null)
        {
            throw new NotImplementedException();
        }

        public void Purge(string queue)
        {
            throw new NotImplementedException();
        }

        public void Remove(string queue)
        {
            throw new NotImplementedException();
        }

        public void Unbind(string queue, string[] exchanges, string[] routeKeys)
        {
            throw new NotImplementedException();
        }
    }
}
