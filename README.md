# Smart Animal Health — GitHub Pages

Web dashboard cho hệ thống:
`ESP32-C3 → ESP32 tổng/Gateway → WiFi → MQTT → Web`

## Triển khai GitHub Pages

1. Tạo một repository mới trên GitHub.
2. Upload toàn bộ nội dung thư mục này, giữ nguyên cấu trúc:
   - `index.html`
   - `css/style.css`
   - `js/app.js`
   - `.nojekyll`
3. Vào **Settings → Pages**.
4. Chọn **Deploy from a branch**, chọn branch chứa `index.html` (thường là `main`) và thư mục `/ (root)`.
5. Mở URL GitHub Pages được GitHub cấp.

## MQTT hiện tại

Web giữ nguyên broker và topic logic của bản đang dùng:

- Broker WebSocket: `wss://broker.hivemq.com:8884/mqtt`
- Data: `esp32_farm/<PROJECT_ID>/data`
- Allowed IDs: `esp32_farm/<PROJECT_ID>/allowed_ids`
- Status: `esp32_farm/<PROJECT_ID>/status`

Project mặc định: `healthier_farm_demo_001`

Danh sách ID và project được lưu bằng `localStorage` của trình duyệt; khi MQTT kết nối, web publish danh sách ID lên topic `allowed_ids` với `retain=true`.

## Lưu ý

Đây là web tĩnh nên GitHub Pages không lưu dữ liệu server-side. Lịch sử realtime trong giao diện vẫn hoạt động theo logic hiện tại của web và được giữ trong bộ nhớ trình duyệt.

File `index.html` đã được tách CSS/JS để dễ quản lý trên GitHub; logic giao diện, MQTT, biểu đồ, dashboard, thiết bị, chi tiết, phân tích, lịch sử, cảnh báo, theme và quản lý ID được giữ nguyên.
