import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.xml.soap.Text;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BucketSortSolver {
    private static final String URL = "tcp://localhost:61616";
    private static final String SPLIT_LIST_QUEUE_NAME = "split-list";
    private static final String SORT_BUCKET_QUEUE_NAME = "sort-bucket";
    private static final String SORTED_BUCKETS_QUEUE_NAME = "sorted-buckets";

    public List<Long> solve(List<Long> list) {
        try {
            // Create ActiveMQ connection and session
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URL);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create ActiveMQ Queues
            Destination splitListIntoBucketsQueue = session.createQueue(SPLIT_LIST_QUEUE_NAME);
            Destination sortBucketsQueue = session.createQueue(SORT_BUCKET_QUEUE_NAME);
            Destination sortedBucketsQueue = session.createQueue(SORTED_BUCKETS_QUEUE_NAME);

            sendListToActiveMQ(list, splitListIntoBucketsQueue, session);
            List<List<Long>> buckets = listenToSendListQueue(splitListIntoBucketsQueue, session);

            sendListToActiveMQ(buckets, sortBucketsQueue, session);

            sendTotalSizeToActiveMQ(list.size(), sortedBucketsQueue, session);

            listenToSortBucketsQueue(sortBucketsQueue, sortedBucketsQueue, session);

            listenToSortedBucketsQueue(sortedBucketsQueue, session);

            session.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    private void sendListToActiveMQ(List list, Destination queue, Session session) throws
            JMSException {
        MessageProducer producer = session.createProducer(queue);
        ObjectMessage message = session.createObjectMessage((Serializable) list);
        producer.send(message);
        producer.close();
    }

    private void sendTotalSizeToActiveMQ(int size, Destination queue, Session session) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage(String.valueOf(size));
        producer.send(message);
        producer.close();
    }

    private List<List<Long>> listenToSendListQueue(Destination queue, Session session) throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive();
        if (message instanceof ObjectMessage) {
            ObjectMessage objectMsg = (ObjectMessage) message;
            List<Long> list = (List<Long>) objectMsg.getObject();
            System.out.println("SendListQueue listener got a list of size " + list.size());

            int nBuckets = (int) Math.sqrt(list.size());
            List<List<Long>> buckets = new ArrayList<>();
            for (int i = 0; i < nBuckets; i++) {
                buckets.add(new ArrayList<>());
            }
            Long highestValue = Util.findMax(list);

            for (Long element : list) {
                int originalBucketIdx = (int) Math.floor(nBuckets * element / (double) highestValue);
                int bucketIdx = element.equals(highestValue) ? originalBucketIdx - 1 : originalBucketIdx;
                buckets.get(bucketIdx).add(element);
            }

            consumer.close();

            return buckets;
        }

        consumer.close();

        return null;
    }

    private void listenToSortBucketsQueue(Destination listenQueue, Destination sendQueue, Session session) throws JMSException {
        MessageConsumer consumer = session.createConsumer(listenQueue);

        Message message = consumer.receive();
        if (message instanceof ObjectMessage) {
            ObjectMessage objectMsg = (ObjectMessage) message;
            List<List<Long>> buckets = (List<List<Long>>) objectMsg.getObject();
            System.out.println("sortBucketsListener got a list of size " + buckets.size());

            MessageProducer producer = session.createProducer(sendQueue);

            Comparator<Long> comparator = Comparator.naturalOrder();
            for (List<Long> bucket : buckets) {
                bucket.sort(comparator);

                ObjectMessage sortedBucketMessage = session.createObjectMessage((Serializable) bucket);
                producer.send(sortedBucketMessage);
            }

            producer.close();
        }

        consumer.close();
    }

    private void listenToSortedBucketsQueue(Destination queue, Session session) throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);
        Integer totalSize = null;
        List<Long> sortedList = new ArrayList<>();

        while (totalSize == null || sortedList.size() < totalSize) {
            Message message = consumer.receive();
            if (message instanceof ObjectMessage) {
                ObjectMessage objectMsg = (ObjectMessage) message;
                List<Long> bucket = (List<Long>) objectMsg.getObject();
                System.out.println("sortedBucketsListener got a list of size " + bucket.size());

                sortedList.addAll(bucket);
            } else if (message instanceof TextMessage) {
                TextMessage textMsg = (TextMessage) message;
                String text = textMsg.getText();
                totalSize = Integer.parseInt(text);
            }
        }

        consumer.close();
    }
}
