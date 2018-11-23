package test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.utils.Utils;

public class KafkaMessageProducer implements Runnable {
	private final Producer<String, String> producer;
	private final String topic;

	public KafkaMessageProducer(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop5:9092,hadoop6:9092,hadoop7:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
		this.topic = topic;
	}

	@Override
	public void run() {
		while (true) {
			Random random = new Random();
			String message = "##0173ST=32;CN=2011;PW=123456;MN=99010004;"
					+ "CP=&&DataTime="
					+ new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
					+ ";001-Rtd=" + random.nextDouble() + ";" + "B01-Rtd="
					+ random.nextDouble() + ";011-Rtd=46.398,011-Flag=N;"
					+ "060-Rtd=" + random.nextDouble() + ",060-Flag=N;065-Rtd="
					+ random.nextDouble() + ",065-Flag=N&&C601";
			System.out.println(message);
			producer.send(new ProducerRecord<String, String>(topic, message));
			Utils.sleep(100);

		}

	}

	public static void main(String[] args) {
		
		for(int i=0;i<50;i++){
			Thread thread = new Thread(new KafkaMessageProducer("netty_message"));
			thread.start();
		}
	}
}
