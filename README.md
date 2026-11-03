## NestJS-Microservice-DLQ

#### agee ei project complete kore---> [NestJS Microservices with Kafka (project)](https://github.com/Omarmdwasimuddin/NestJS-Microservice-Kafka)


####
```bash
docker compose up -d
```
---


#### `notification-service/src/app/app.controller.ts`
```ts
import { Controller, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @EventPattern('user_created')
  async handleUserCreated(@Payload() data: any) {
    try {
      console.log('Main Event Received:', data);
      throw new Error('Payment service is Failed!')
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.log('Sending Event to DLQ');
      this.kafkaClient.emit('user_created_dlq', {
        failedData: data,
        error: errorMessage,
        failedAt: new Date()
      })
    }
  }
  @EventPattern('user_created_dlq')
  handleDLQ(@Payload() data: any) {
    console.log('DLQ Event Received:', data);
  }
}
```
---
