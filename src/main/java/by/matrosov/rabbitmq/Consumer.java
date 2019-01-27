package by.matrosov.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static final String EXCHANGE = "exchange_direct";
    private static final String QUEUE = "queue";

    public Consumer() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String bindingKey = "prime";
        channel.exchangeDeclare(EXCHANGE,"direct");
        channel.queueBind(QUEUE, EXCHANGE, bindingKey);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        boolean noAck = true;
        channel.basicConsume(QUEUE, noAck, deliverCallback, consumerTag -> { });
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        new Consumer();
    }
}
