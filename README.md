# Building_DataLakeHouse_With_Open_Source
Tiểu luận chuyên ngành

## Mô tả tổng quan
Đây là mô tả về kiến trúc và các công nghệ được sử dụng trong hệ thống xử lý Big Data.

![workflow](https://github.com/lephuocyen20/Build-a-Data-Platform-with-Open-Source/assets/134000205/6b3a8051-cb7b-4f95-9f25-25abeeb7d68a)

## Dữ liệu đầu vào (Data source)
Dữ liệu thô được lưu trữ trong CSDL MySQL.

Sử dụng MySQL để quản lý và lưu trữ dữ liệu thô thông qua các bảng quan hệ.

## Quá trình xử lý dữ liệu (Data processing)
### Ingestion
Sử dụng thư viện Polars của Python để tải dữ liệu từ MySQL vào layer Bronze trong Data LakeHouse.
### Transforms Data
Sử dụng Apache Spark để chuyển đổi, làm sạch dữ liệu từ Bronze thành các layer Silver/Gold.
## Lưu trữ dữ liệu (Data storage)
Sử dụng Minio để lưu trữ dữ liệu dưới dạng object.

Sử dụng Apache Iceberg để định dạng lại dữ liệu thành dạng table.

Sử dụng HMS để lưu trữ metadata của các bảng dữ liệu.

![Player_Data](https://github.com/lephuocyen20/Build-a-Data-Platform-with-Open-Source/assets/134000205/576aead4-26f8-4e6d-b47e-3807e95ed4b0)

## Trực quan hóa và phân tích dữ liệu (Visualization and analysis)
Sử dụng Apache Superset để thực hiện trực quan hóa dữ liệu và xây dựng báo cáo.

Ap dụng Linear Regression để dự đoán giá cả sản phẩm.

## Điều phối công việc (Orchestration)
Sử dụng Dagster để quản lý và điều phối các tác vụ xử lý dữ liệu.

![Data_Lineage](https://github.com/lephuocyen20/Build-a-Data-Platform-with-Open-Source/assets/134000205/5895d876-7646-4d05-8aed-b199080c2207)

## Môi trường triển khai
Triển khai toàn bộ hệ thống trên nền tảng Docker để dễ dàng mở rộng.
