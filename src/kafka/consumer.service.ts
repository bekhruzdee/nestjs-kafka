import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import {
  Kafka,
  Consumer,
  ConsumerSubscribeTopics,
  ConsumerRunConfig,
} from 'kafkajs';

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
  private readonly kafka = new Kafka({
    clientId: 'nestjs-consumer',
    brokers: ['localhost:9092'],
    retry: { retries: 8, initialRetryTime: 300 },
  });

  private readonly consumers: Consumer[] = [];

  async consume(
    topicConfig: ConsumerSubscribeTopics,
    runConfig: ConsumerRunConfig,
  ) {
    const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka' });

    await consumer.connect();
    console.log(`Consumer connected for group: nestjs-kafka`);

    await consumer.subscribe(topicConfig);

    await consumer.run({
      eachMessage: async (payload) => {
        const { topic, partition, message, heartbeat, pause } = payload;

        // heartbeat ni vaqti-vaqti bilan chaqirib turish kerak (uzoq ishlov berishda)
        // lekin oddiy holatda bu avtomatik ishlaydi, shuning uchun majburiy emas

        const value = message.value?.toString() || '';

        console.log('─'.repeat(50));
        console.log(`Received message:`);
        console.log(`  Topic:     ${topic}`);
        console.log(`  Partition: ${partition}`);
        console.log(`  Offset:    ${message.offset}`);
        console.log(`  Value:     ${value}`);
        console.log('─'.repeat(50));

        // agar juda uzoq ishlov bersangiz (masalan 30+ soniya)
        // await heartbeat(); ni qo‘shish tavsiya etiladi
      },
    });

    this.consumers.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
    console.log('All consumers disconnected');
  }
}
