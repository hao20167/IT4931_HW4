# IT4931 – HW4: Data pipeline theo kiến trúc Lambda

Thống kê COUNT và SUM(amount_cents) theo vùng đón khách trên dữ liệu NYC TLC Yellow Taxi 01/2025.

- **A – batch-only**: Python producer → Kafka → raw consumer (Parquet) → Spark batch 30 s → PostgreSQL.
- **B – Lambda**: như A, thêm Spark Structured Streaming đọc Kafka và ghi idempotent vào PostgreSQL. Serving trả `BatchAgg(seq ≤ H) + SpeedEvents(seq > H)`.

Báo cáo: [report/Bao_cao.pdf](report/Bao_cao.pdf).

## Cấu trúc thư mục

| Đường dẫn | Nội dung |
|---|---|
| `compose.yaml`, `Dockerfile`, `requirements.txt` | Ba container `kafka`, `db`, `app`; phiên bản thư viện cố định |
| `src/download.py` | Tải file nguồn, ghi URL, ngày tải, SHA-256 vào `data/download.json` |
| `src/prepare.py` | Làm sạch, chuẩn hóa 20.000 sự kiện, ghi `data/events.jsonl`, `data/sample.jsonl`, `data/manifest.json` |
| `src/run.py` | Chạy một lượt: đặt lại trạng thái, khởi động raw consumer, Spark batch, Spark streaming (B), replay producer, đo freshness/backlog, đối chiếu ground truth |
| `src/raw.py` | Raw consumer: Kafka → file Parquet bất biến, công bố mốc H |
| `src/engine.py` | Spark batch (`batch`) và Spark Structured Streaming (`speed`) |
| `src/common.py` | Kết nối PostgreSQL, ghi `speed_events` idempotent |
| `src/check.py` | Kiểm tra 16 lượt đúng và ghi lại sự kiện không làm đổi kết quả |
| `src/summarize.py` | Tạo `metrics/summary.csv`, `metrics/correctness.csv` và 4 biểu đồ |
| `run_all.py` | Chạy tuần tự toàn bộ E1–E5, đặt giới hạn CPU/RAM và ghi `resources.csv` |
| `sql/schema.sql`, `sql/queries.sql` | Schema serving, view Q1/Q2/visible và truy vấn kiểm thử |
| `data/manifest.json`, `data/download.json`, `data/sample.jsonl` | Nhật ký nguồn, quy tắc làm sạch, 1.000 sự kiện mẫu |
| `metrics/` | Số liệu thô từng lượt, bảng tổng hợp, biểu đồ |
| `logs/` | Log chạy thí nghiệm, trạng thái dịch vụ, kết quả Q1/Q2, kiểm tra correctness |
| `report/build.py` | Tạo lại báo cáo PDF từ `metrics/` |

## Môi trường đã đo

- MacBook Pro Apple M1 Pro 10 lõi, RAM 32 GB; Docker Desktop cấp 10 CPU, 7,65 GiB RAM.
- Spark 3.5.3 (local mode, image `apache/spark:3.5.3`), Python 3.8.10, Java 11.0.24, Kafka 3.9.0 (KRaft, 1 topic `events`, 1 partition), PostgreSQL 16.6.
- Giới hạn tài nguyên:

| Container | C1 | C2 |
|---|---|---|
| `kafka` | 1 CPU / 1 GiB | 1 CPU / 1 GiB |
| `db` | 1 CPU / 512 MiB | 1 CPU / 512 MiB |
| `app` (producer, raw consumer, 2 job Spark, script đo) | 2 CPU / 2 GiB | 4 CPU / 4 GiB |
| Mỗi job Spark | `local[1]`, driver memory 512 MB | `local[2]`, driver memory 1 GB |

E1–E4 dùng C1, E5 dùng cả C1 và C2.

## Cài đặt, thu thập và làm sạch dữ liệu

Yêu cầu: Docker Desktop (Docker Compose v2), Python 3 trên máy host để chạy `run_all.py`.

```sh
docker compose build
docker compose up -d
docker compose exec -T app python3 src/download.py
docker compose exec -T app python3 src/prepare.py
```

Quy tắc làm sạch (ghi trong `data/manifest.json`): thời điểm đón thuộc tháng 1/2025; `PULocationID` trong 1–265; `total_amount` không rỗng, hữu hạn, trong [0; 10.000] USD; loại bản ghi trùng theo SHA-256 của toàn bộ trường gốc; tiền đổi sang cent, làm tròn half-up. Lấy 20.000 bản ghi hợp lệ đầu tiên theo thứ tự file nên không có seed.

## Chạy toàn bộ thí nghiệm

```sh
python3 run_all.py
```

`run_all.py` chạy tuần tự 16 lượt (E1, E2: A và B × 2; E3 × 2; E4 × 2; E5: C1 × 2, C2 × 2). Trước mỗi lượt nó đặt giới hạn container `app` bằng `docker update`, ghi `docker stats` vào `metrics/<lượt>/resources.csv`, rồi gọi `src/run.py`. Lượt nào đã có `metrics/<lượt>/result.json` sẽ được bỏ qua; muốn chạy lại thì xóa thư mục của lượt đó.

## Chạy riêng một lượt

Đặt giới hạn cho container `app` trước (C1: 2, C2: 4):

```sh
docker update --cpus 2 --memory 2g --memory-swap 2g $(docker compose ps -q app)
```

Sau đó:

```sh
docker compose exec -T app python3 src/run.py A E1 1 2 0        # E1, batch-only, lượt 1, C1
docker compose exec -T app python3 src/run.py B E2 1 2 0        # E2, Lambda
docker compose exec -T app python3 src/run.py B E3 1 2 0        # E3: dừng speed job ở giây 40 trong 30 s
docker compose exec -T app python3 src/run.py B E4 1 2 1        # E4: trigger speed 1 s
docker compose exec -T app python3 src/run.py B E5 1 4 0 150    # E5 với C2 (nhớ docker update --cpus 4 --memory 4g)
```

Tham số: `<phương án A|B> <thí nghiệm> <lượt> <số CPU của app: 2|4> <cải tiến: 0|1> [rate]`. Rate mặc định: E1 = 20, E5 = 150, còn lại 100 sự kiện/s; mỗi lượt phát trong 120 s.

- **Đặt lại dữ liệu**: `src/run.py` tự xóa và tạo lại topic Kafka, chạy lại `sql/schema.sql` (xóa toàn bộ bảng), xóa `data/raw`, `data/checkpoint` và mốc H trước mỗi lượt. `data/events.jsonl` giữ nguyên.
- **E4** chỉ đổi `processingTime` của speed job từ 2 s sang 1 s (`src/engine.py`, tham số cải tiến = 1).
- **E5** chỉ đổi giới hạn của container `app` và số lõi/driver memory của Spark; Kafka, PostgreSQL, dữ liệu và mã xử lý giữ nguyên.

## Kiểm thử Q1/Q2 và correctness

```sh
docker compose exec -T db psql -U postgres -d hw4 -f - < sql/queries.sql
docker compose exec -T app python3 src/check.py
```

Q1 là view `q1` (COUNT/SUM theo category), Q2 là view `q2` (tổng toàn hệ thống). View `visible` trả các `event_id` đang đóng góp vào Q1/Q2 và được dùng để đo freshness.

## Số liệu và biểu đồ

```sh
docker compose exec -T app python3 src/summarize.py
docker compose exec -T app python3 report/build.py
```

Tên lượt có dạng `<thí nghiệm>_<phương án>_C<số CPU của app>_<cải tiến>_<lượt>`; ví dụ `E5_B_C4_0_1` là E5, Lambda, cấu hình C2 (4 CPU), lượt 1. Mỗi thư mục lượt gồm:

- `timeline.csv`: mỗi 0,5 s ghi số sự kiện đã phát, số visible, backlog, COUNT, SUM.
- `freshness.csv`: event_id mẫu, thời điểm phát (tính từ đầu lượt), freshness.
- `resources.csv`: CPU và RAM của từng container khoảng 1 lần/giây.
- `sql.csv`: thời gian phản hồi Q1, Q2 ở mỗi lần poll.
- `batch.csv`: thời điểm, mốc H và thời gian mỗi lần batch.
- `sent.jsonl`: các sự kiện đã phát thành công (dùng làm ground truth).
- `speed.log`, `batch.log`, `result.json`.

Quy ước đo: poll 0,5 s; bỏ 20 s đầu làm warm-up, chỉ tổng hợp trong giây 20–120; CPU tính theo % một lõi; số trong báo cáo là trung bình hai lượt.

Biểu đồ dùng font Times New Roman nếu có `assets/Times.ttf` và `assets/TimesBold.ttf` (không kèm trong repo); nếu không có sẽ dùng DejaVu Serif.

```sh
docker compose down
```
