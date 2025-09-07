# Kafka Practice Project
이 프로젝트는 Apache Kafka의 핵심 기능을 이해하고 실습하기 위해 만들어진 자바 기반의 학습용 레파지토리입니다. 기본적인 Producer, Consumer부터 시작하여 파티셔닝, 커밋 전략, 데이터 직렬화 등 고급 주제까지 다양한 예제 코드를 포함하고 있습니다.

---

### 🚀 프로젝트 목표
- Kafka Producer의 동작 방식(동기/비동기, 콜백, 파티셔너) 이해
- Kafka Consumer의 동작 방식(자동/수동 커밋, 리밸런스, 특정 파티션 할당) 이해
- 커스텀 직렬화/역직렬화(Serde)를 통한 객체 데이터 처리
- 파일 I/O와 데이터베이스 연동을 포함한 간단한 데이터 파이프라인 구축 실습
  
---

### 🛠️ 기술 스택 및 환경
- Language: Java 17(Local), Java 11(Kafka Server)
- O/S: Linux 22.04 LTS, Windows11
- Build Tool: Gradle
- Kafka Version: 7.1.2 (Confluent Kafka)
- IDE: IntelliJ IDEA
  
---

### 📁 프로젝트 구조
kafkaproj-01/ <br>
├── 📄 README.md <br>
├── 📦 producers/      # Kafka Producer 예제 코드 <br>
├── 📦 consumers/      # Kafka Consumer 예제 코드 <br>
└── 📦 practice/       # 파일/DB 연동 등 응용 실습 코드 <br>

---

## 📖 모듈별 상세 설명 및 실행 가이드
### 1️⃣ Producers 모듈
Kafka Producer의 다양한 메시지 전송 방법을 학습한다.

| 파일명 | 설명 |
|--------|------|
| SimpleProducer.java | 기본 메시지 전송 (fire-and-forget) |
| SimpleProducerSync.java | `get()`으로 동기 전송 |
| SimpleProducerASync.java | Callback 기반 비동기 전송 |
| ProducerASyncWithKey.java | 메시지 Key를 통한 파티션 지정 |
| ProducerASyncCustomCB.java | CustomCallback 클래스로 전송 결과 처리 |
| PizzaProducer... | CustomPartitioner로 조건별 파티션 지정 |


### 2️⃣ Consumers 모듈
Kafka Consumer의 다양한 옵션과 전략을 학습한다.

| 파일명 | 설명 |
|--------|------|
| SimpleConsumer.java | 기본 Consumer (자동 커밋) |
| ConsumerCommit.java | commitSync / commitAsync 수동 커밋 |
| ConsumerPartitionAssign.java | 특정 파티션만 구독 |
| ConsumerPartitionAssignSeek.java | 특정 오프셋부터 메시지 읽기 |
| ConsumerMTopicRebalance.java | 다중 토픽 구독 + 리밸런스 리스너 |
| ConsumerWakeup.java | `wakeup()`을 통한 안전한 종료 |


### 3️⃣ Practice 모듈
파일 I/O 및 DB 연동을 통해 실전과 유사한 시나리오를 다룬다.

| 파일명 | 설명 |
|--------|------|
| FileEventSource / FileProducer | 텍스트 파일(`pizza_sample.txt`) 내용을 라인별 메시지로 전송 |
| FileToDBConsumer | 메시지를 DB(H2 In-Memory)에 저장 |
| OrderSerializer / OrderDeserializer | `OrderModel` 객체 직렬화/역직렬화 |
| OrderSerdeProducer / OrderSerdeConsumer | Serde 기반 객체 메시징 구현 |

---
