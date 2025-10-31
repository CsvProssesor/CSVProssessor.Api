# Service Bus & Message Broker - Hướng Dẫn Từng Bước

## A. LÝ THUYẾT

### I. Cách hoạt động của Message Broker

#### 1. Queue (Hàng Đợi)
- **Định nghĩa:** 1 message cho 1 receiver
- **Ví dụ:** Message được gửi đi, service A nhận và xử lý logic.
- **Đặc điểm:** Nếu có nhiều consumer cùng subscribe queue, mỗi message chỉ đến 1 trong số họ (giống như lấy số thứ tự ở ngân hàng, ai rảnh thì xử lý).

#### 2. Topic (Chủ Đề - Giống Như Loa Phát Thanh)
- **Định nghĩa:** 1 message cho nhiều receiver
- **Ví dụ:** Message được gửi đi, service nào có subscribe thì sẽ được xử lý.

---

### II. Các Khái Niệm Cơ Bản

#### 1. Producer (Publisher) - Người Gửi Thư
- Là người đưa thư, người gửi message
- Gửi message vào queue hoặc topic

#### 2. Consumer (Subscriber) - Người Nhận Thư
- Là người nhận thư, đọc message và xử lý logic code
- Lắng nghe và xử lý các message nhận được

#### 3. Queue - Thùng Thư
- Là hàng đợi message cho đến khi có 1 Consumer đến subscribe để nhận message
- Nếu có nhiều consumer cùng subscribe queue, mỗi message chỉ đến 1 trong số họ

#### 4. Exchange - Đài Phát Thanh
- Là nơi định tuyến message đến các queue phù hợp
- Quyết định message nào sẽ đi vào queue nào cho service subscribe
- Mỗi consumer có queue riêng, cùng subscribe vào 1 topic/exchange, nên tất cả đều nhận được bản sao của message

**Các loại Exchange trong RabbitMQ:**

| Loại | Mô Tả |
|------|-------|
| **Direct** | Định tuyến các message dựa trên routing KEY. Các queue sẽ nhận message đúng chính xác queue đó. |
| **Fanout** | Định tuyến các message tới nhiều queue. Tất cả message được gửi tới tất cả các queue đã bind với exchange này. |
| **Topic** | Các message sẽ được định tuyến theo 1 pattern nhất định của queue đó. Ví dụ: queue có pattern `*.Bombay.*` thì message với routing key `User.Bombay.Message` sẽ đến queue đó. |
| **Header** | Sử dụng message header attribute cho việc routing. |

---

### III. Ưu Điểm (Advantage)

✅ **KHÔNG MẤT DỮ LIỆU**
- Nếu 1 service bị tắt, message vẫn nằm trong queue
- Khi service khởi động lại, sẽ tiếp tục xử lý mà không cần request lại

✅ **TÍNH MỞ RỘNG CAO**
- Có thể dùng Topic cho nhiều consumer đăng ký để xử lý logic cùng lúc
- Tải được phân tán, xử lý nhanh hơn

✅ **TÁCH BIỆT LOGIC**
- Service gửi message và consume message có thể viết bằng nhiều ngôn ngữ khác nhau
- Hỗ trợ: C#, Java, Python, Node.js, ...

---

## B. SYNTAX VÀ PSEUDO CODE

### I. Producer (Người Gửi Message)

**Kết nối và gửi message vào queue:**

```csharp
// Kết nối tới RabbitMQ
var connection = CreateConnection();
var channel = connection.CreateChannel();

// Gửi message vào queue
channel.BasicPublish(
    exchange: "",                    // Tên exchange. "" = gửi trực tiếp vào queue
    routingKey: "csv-import-queue",  // Tên queue muốn gửi message vào
    body: messageBytes               // Nội dung message (dạng byte[])
);
```

**Giải thích các thuộc tính:**
- `exchange`: Tên của exchange. Nếu để trống (`""`), message sẽ đi thẳng vào queue theo routingKey
- `routingKey`: Tên của queue hoặc topic mà message sẽ được gửi đến
- `body`: Dữ liệu thực tế của message (thường là JSON hoặc byte[])

---

### II. Consumer (Người Nhận Message)

**Kết nối và nhận message từ queue:**

```csharp
// Kết nối tới RabbitMQ
var connection = CreateConnection();
var channel = connection.CreateChannel();

// Đăng ký nhận message từ queue
channel.BasicConsume(
    queue: "csv-import-queue",       // Tên queue muốn nhận message
    autoAck: false,                  // Có tự động xác nhận đã nhận message không
    consumer: myConsumer             // Đối tượng xử lý message nhận được
);
```

**Giải thích các thuộc tính:**
- `queue`: Tên queue muốn nhận message
- `autoAck`: 
  - `true`: Message được coi là đã xử lý ngay khi nhận
  - `false` (khuyến nghị): Phải gọi `BasicAck` sau khi xử lý xong để đảm bảo không mất dữ liệu
- `consumer`: Hàm hoặc đối tượng sẽ xử lý message khi nhận được

---

### III. Exchange & Topic (Định Tuyến Nâng Cao)

**Tạo exchange kiểu topic:**

```csharp
// Tạo exchange
channel.ExchangeDeclare(
    exchange: "csv-changes-topic",   // Tên exchange
    type: "topic",                   // Kiểu exchange (topic, direct, fanout, ...)
    durable: true                    // Có lưu lại khi restart broker không
);

// Bind queue vào exchange với routing key
channel.QueueBind(
    queue: "myQueue",                // Tên queue
    exchange: "csv-changes-topic",   // Tên exchange
    routingKey: "csv.*"              // Pattern routing (csv.import, csv.export, ...)
);
```

**Giải thích các thuộc tính:**
- `exchange`: Tên exchange (nơi phát tán message)
- `type`: Kiểu exchange (`topic`, `direct`, `fanout`, `header`)
- `durable`: Nếu `true`, exchange sẽ tồn tại sau khi broker restart
- `queue`: Tên queue sẽ nhận message từ exchange
- `routingKey`: Pattern để lọc message
  - `csv.*` → nhận `csv.import`, `csv.export`, ...
  - `csv.import` → nhận chính xác `csv.import`