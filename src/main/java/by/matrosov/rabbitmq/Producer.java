package by.matrosov.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String EXCHANGE = "exchange_direct";
    private static final String QUEUE = "queue";

    public Producer() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()){

            boolean durable = true;
            boolean exclusive = false;
            boolean autodelete = false;

            channel.exchangeDeclare(EXCHANGE, "direct");
            channel.queueDeclare(QUEUE, durable, exclusive, autodelete, null);

            String routingKey;
            String message;
            Random random = new Random();
            for (int i = 1; i < 25; i++) {
                int n = random.nextInt(i) + 1;
                message = String.valueOf(n);
                if (isPrime(n)){
                    routingKey = "prime";
                }else {
                    routingKey = "non-prime";
                }

                channel.basicPublish(EXCHANGE, routingKey, null, message.getBytes());
                System.out.println("[x] Sent '" + routingKey + ": " + message + "'");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private boolean isPrime(int n){
        if (n % 2 == 0) return false;
        for (int i = 3; i*i <= n; i+= 2) {
            if (n % i == 0)
                return false;
        }
        return true;
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        new Producer();
    }
}
