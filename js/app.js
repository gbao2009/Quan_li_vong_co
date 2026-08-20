// --- CẤU HÌNH MQTT ---
// Sửa thành thông tin broker của bạn nếu cần
const MQTT_BROKER = "broker.emqx.io";
const MQTT_PORT = 8083; // Websocket port
const MQTT_TOPIC_SUB = "farm/system/data/#"; // Topic nhận dữ liệu từ ESP32 Tổng

let mqttClient;
let managedGateways = JSON.parse(localStorage.getItem('managedGateways')) || {};
let currentGatewayId = null;

// Biến lưu trữ biểu đồ
let healthChart, tempChart, accelChart;

// Dữ liệu Realtime để vẽ biểu đồ
const maxDataPoints = 30;
const chartData = {
    labels: [],
    bpm: [], spo2: [], temp: [],
    accelX: [], accelY: [], accelZ: [], accelTotal: []
};

// Từ điển dịch trạng thái
const STATE_TRANSLATION = {
    "MOVING": "DI CHUYỂN",
    "STILL": "ĐỨNG YÊN",
    "WALKING": "ĐI BỘ",
    "RUNNING": "CHẠY"
};

// --- KHỞI TẠO HỆ THỐNG ---
window.onload = function() {
    initCharts();
    loadGatewaysToSelect();
    renderGatewayList();
    connectMQTT();
    
    // Cập nhật thời gian offline mỗi giây
    setInterval(checkGatewayTimeout, 10000); 
};

// --- UI CHUYỂN TAB ---
function switchTab(tab) {
    document.querySelectorAll('.nav-links li').forEach(el => el.classList.remove('active'));
    event.currentTarget.classList.add('active');
    
    document.getElementById('tab-dashboard').style.display = tab === 'dashboard' ? 'block' : 'none';
    document.getElementById('tab-settings').style.display = tab === 'settings' ? 'block' : 'none';
    document.getElementById('dashboard-controls').style.display = tab === 'dashboard' ? 'block' : 'none';
    
    if(tab === 'dashboard') document.getElementById('page-title').innerText = "Bảng điều khiển";
    if(tab === 'settings') document.getElementById('page-title').innerText = "Quản lý ESP32 Tổng";
}

// --- QUẢN LÝ ESP32 TỔNG (CRUD) ---
function saveGateway(e) {
    e.preventDefault();
    const id = document.getElementById('set-gw-id').value.trim();
    if(!id) return;

    managedGateways[id] = {
        name: document.getElementById('set-gw-name').value || id,
        desc: document.getElementById('set-gw-desc').value,
        alert_bpm: parseFloat(document.getElementById('set-gw-bpm').value),
        alert_spo2: parseFloat(document.getElementById('set-gw-spo2').value),
        alert_temp: parseFloat(document.getElementById('set-gw-temp').value),
        last_seen: 0
    };

    localStorage.setItem('managedGateways', JSON.stringify(managedGateways));
    document.getElementById('gatewayForm').reset();
    loadGatewaysToSelect();
    renderGatewayList();
    showToast(`Đã lưu thành công ESP32 Tổng: ${id}`);
}

function deleteGateway(id) {
    if(confirm(`Bạn có chắc muốn xóa ${id}?`)) {
        delete managedGateways[id];
        localStorage.setItem('managedGateways', JSON.stringify(managedGateways));
        loadGatewaysToSelect();
        renderGatewayList();
        if(currentGatewayId === id) changeGateway(); // Reset view
    }
}

function renderGatewayList() {
    const container = document.getElementById('gatewayListContainer');
    container.innerHTML = '';
    for (const [id, info] of Object.entries(managedGateways)) {
        container.innerHTML += `
            <div class="gw-list-item">
                <div>
                    <strong>${info.name}</strong> <br>
                    <small>ID: ${id}</small> | <small>Ngưỡng Nhiệt: ${info.alert_temp}°C</small>
                </div>
                <button onclick="deleteGateway('${id}')"><i class="fa-solid fa-trash"></i> Xóa</button>
            </div>
        `;
    }
}

function loadGatewaysToSelect() {
    const select = document.getElementById('gatewaySelect');
    select.innerHTML = '<option value="">-- Chọn thiết bị --</option>';
    for (const [id, info] of Object.entries(managedGateways)) {
        select.innerHTML += `<option value="${id}">${info.name} (${id})</option>`;
    }
    
    if(!currentGatewayId && Object.keys(managedGateways).length > 0) {
        select.value = Object.keys(managedGateways)[0];
        changeGateway();
    } else {
        select.value = currentGatewayId;
    }
}

function changeGateway() {
    currentGatewayId = document.getElementById('gatewaySelect').value;
    document.getElementById('st-gateway-id').innerText = currentGatewayId || "Chưa chọn";
    
    // Reset bảng dữ liệu và biểu đồ khi chuyển thiết bị
    resetUI();
}

// --- XỬ LÝ MQTT ---
function connectMQTT() {
    const clientId = "web_client_" + Math.random().toString(16).substr(2, 8);
    mqttClient = new Paho.MQTT.Client(MQTT_BROKER, MQTT_PORT, clientId);
    
    mqttClient.onConnectionLost = (res) => {
        console.log("MQTT Disconnected: " + res.errorMessage);
        setTimeout(connectMQTT, 3000);
    };
    
    mqttClient.onMessageArrived = onMessageArrived;
    
    mqttClient.connect({
        onSuccess: () => {
            console.log("MQTT Connected!");
            mqttClient.subscribe(MQTT_TOPIC_SUB);
        }
    });
}

function onMessageArrived(message) {
    try {
        const data = JSON.parse(message.payloadString);
        
        // LUẬT CỐT LÕI: Phải có gateway_id
        if(!data.gateway_id) return;
        const gwId = data.gateway_id;
        
        // Nếu Gateway chưa được đăng ký trong hệ thống, bỏ qua (hoặc bạn có thể tự động thêm tùy ý)
        if(!managedGateways[gwId]) return;

        // Cập nhật last_seen
        managedGateways[gwId].last_seen = Date.now();

        // Chỉ cập nhật UI nếu dữ liệu thuộc về ESP32 Tổng đang được chọn xem
        if(gwId === currentGatewayId) {
            updateDashboard(data);
            updateCharts(data);
            addHistoryRow(data);
            checkAlerts(gwId, data);
        }

    } catch (e) {
        console.error("Lỗi parse JSON: ", e);
    }
}

// --- CẬP NHẬT GIAO DIỆN CHÍNH ---
function updateDashboard(data) {
    // Cập nhật trạng thái kết nối
    setConnectionStatus('st-status', true, 'Đang kết nối');
    setConnectionStatus('st-wifi', true, 'Đã kết nối');
    setConnectionStatus('st-mqtt', true, 'Đã kết nối');
    
    // Logic BLE Vòng cổ: Giả sử nếu có gửi dữ liệu BPM/Temp lên nghĩa là đã kết nối
    const bleConnected = data.bpm > 0 || data.temperature > 0;
    setConnectionStatus('st-ble', bleConnected, bleConnected ? 'Đã kết nối vòng cổ' : 'Chưa kết nối');
    
    document.getElementById('st-time').innerText = new Date().toLocaleTimeString('vi-VN');

    // Cập nhật thông số Cảm biến
    if(data.bpm) document.getElementById('val-bpm').innerText = data.bpm;
    if(data.spo2) document.getElementById('val-spo2').innerText = data.spo2;
    if(data.temperature) document.getElementById('val-temp').innerText = data.temperature.toFixed(2);
    
    if(data.accelTotal) document.getElementById('val-accel').innerText = data.accelTotal.toFixed(2);
    if(data.velocityKmh) document.getElementById('val-vel').innerText = data.velocityKmh.toFixed(2);
    if(data.distance) document.getElementById('val-dist').innerText = data.distance.toFixed(2);
    
    if(data.movementState) {
        document.getElementById('val-state').innerText = STATE_TRANSLATION[data.movementState] || data.movementState;
    }
}

function setConnectionStatus(elementId, isConnected, text) {
    const el = document.getElementById(elementId);
    if(isConnected) {
        el.innerHTML = `<span class="dot green"></span> ${text}`;
        el.className = "dot-text green-text";
    } else {
        el.innerHTML = `<span class="dot red"></span> ${text}`;
        el.className = "dot-text red-text";
    }
}

// Check nếu quá 15s không nhận được data -> báo Mất kết nối
function checkGatewayTimeout() {
    if(!currentGatewayId || !managedGateways[currentGatewayId]) return;
    const lastSeen = managedGateways[currentGatewayId].last_seen;
    if(Date.now() - lastSeen > 15000) {
        setConnectionStatus('st-status', false, 'Mất kết nối');
        setConnectionStatus('st-wifi', false, 'Mất kết nối');
    }
}

// --- BIỂU ĐỒ REALTIME (CHART.JS) ---
function initCharts() {
    const ctxHealth = document.getElementById('healthChart').getContext('2d');
    const ctxTemp = document.getElementById('tempChart').getContext('2d');
    const ctxAccel = document.getElementById('accelChart').getContext('2d');

    // Gradient màu
    const gradientRed = ctxHealth.createLinearGradient(0, 0, 0, 400);
    gradientRed.addColorStop(0, 'rgba(239, 71, 111, 0.5)');
    gradientRed.addColorStop(1, 'rgba(239, 71, 111, 0.0)');

    const gradientOrange = ctxTemp.createLinearGradient(0, 0, 0, 400);
    gradientOrange.addColorStop(0, 'rgba(251, 133, 0, 0.5)');
    gradientOrange.addColorStop(1, 'rgba(251, 133, 0, 0.0)');

    Chart.defaults.font.family = 'Inter';
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 400, easing: 'linear' },
        scales: { x: { display: false }, y: { beginAtZero: false } },
        elements: { point: { radius: 0 }, line: { tension: 0.4 } } // Đường cong mượt
    };

    healthChart = new Chart(ctxHealth, {
        type: 'line',
        data: { labels: chartData.labels, datasets: [
            { label: 'BPM', data: chartData.bpm, borderColor: '#ef476f', backgroundColor: gradientRed, fill: true, yAxisID: 'y' },
            { label: 'SpO₂ (%)', data: chartData.spo2, borderColor: '#3a86ff', fill: false, yAxisID: 'y1' }
        ]},
        options: { ...commonOptions, scales: { 
            y: { position: 'left', min: 40, max: 200 }, 
            y1: { position: 'right', min: 80, max: 100 }
        }}
    });

    tempChart = new Chart(ctxTemp, {
        type: 'line',
        data: { labels: chartData.labels, datasets: [
            { label: 'Nhiệt độ (°C)', data: chartData.temp, borderColor: '#fb8500', backgroundColor: gradientOrange, fill: true }
        ]},
        options: { ...commonOptions, scales: { y: { min: 30, max: 45 } } }
    });

    accelChart = new Chart(ctxAccel, {
        type: 'line',
        data: { labels: chartData.labels, datasets: [
            { label: 'Trục X', data: chartData.accelX, borderColor: '#ef476f', borderWidth: 1, fill: false },
            { label: 'Trục Y', data: chartData.accelY, borderColor: '#06d6a0', borderWidth: 1, fill: false },
            { label: 'Trục Z', data: chartData.accelZ, borderColor: '#3a86ff', borderWidth: 1, fill: false },
            { label: 'Gia tốc Tổng', data: chartData.accelTotal, borderColor: '#8338ec', borderWidth: 2, fill: false }
        ]},
        options: commonOptions
    });
}

function updateCharts(data) {
    const timeNow = new Date().toLocaleTimeString('vi-VN');
    
    if(chartData.labels.length > maxDataPoints) {
        chartData.labels.shift();
        chartData.bpm.shift(); chartData.spo2.shift(); chartData.temp.shift();
        chartData.accelX.shift(); chartData.accelY.shift(); chartData.accelZ.shift(); chartData.accelTotal.shift();
    }

    chartData.labels.push(timeNow);
    chartData.bpm.push(data.bpm || 0);
    chartData.spo2.push(data.spo2 || 0);
    chartData.temp.push(data.temperature || 0);
    
    chartData.accelX.push(data.accelX || 0);
    chartData.accelY.push(data.accelY || 0);
    chartData.accelZ.push(data.accelZ || 0);
    chartData.accelTotal.push(data.accelTotal || 0);

    healthChart.update();
    tempChart.update();
    accelChart.update();
}

function resetUI() {
    chartData.labels = []; chartData.bpm = []; chartData.spo2 = []; chartData.temp = [];
    chartData.accelX = []; chartData.accelY = []; chartData.accelZ = []; chartData.accelTotal = [];
    if(healthChart) { healthChart.update(); tempChart.update(); accelChart.update(); }
    document.getElementById('historyBody').innerHTML = '';
}

// --- LỊCH SỬ BẢNG DỮ LIỆU ---
function addHistoryRow(data) {
    const tbody = document.getElementById('historyBody');
    const time = new Date().toLocaleTimeString('vi-VN');
    const stateStr = STATE_TRANSLATION[data.movementState] || "Chưa rõ";
    
    const row = document.createElement('tr');
    row.innerHTML = `
        <td>${time}</td>
        <td><strong>${data.gateway_id}</strong></td>
        <td>${data.bpm || '--'}</td>
        <td>${data.spo2 ? data.spo2 + '%' : '--'}</td>
        <td style="color:var(--c-orange); font-weight:600;">${data.temperature ? data.temperature.toFixed(2) + '°C' : '--'}</td>
        <td>${data.accelTotal ? data.accelTotal.toFixed(2) : '--'}</td>
        <td>${data.velocityKmh ? data.velocityKmh.toFixed(2) + ' km/h' : '--'}</td>
        <td>${stateStr}</td>
    `;
    
    tbody.prepend(row);
    if(tbody.children.length > 50) tbody.removeChild(tbody.lastChild); // Giữ tối đa 50 dòng
}

// --- CẢNH BÁO (ALERTS) ---
function checkAlerts(gwId, data) {
    const config = managedGateways[gwId];
    if(!config) return;

    if(data.bpm > config.alert_bpm) {
        showToast(`⚠ Nhịp tim cao! ESP32 Tổng: ${gwId} - Hiện tại: ${data.bpm} BPM`);
    }
    if(data.spo2 > 0 && data.spo2 < config.alert_spo2) {
        showToast(`⚠ SpO₂ thấp! ESP32 Tổng: ${gwId} - Hiện tại: ${data.spo2}%`);
    }
    if(data.temperature > config.alert_temp) {
        showToast(`⚠ Nhiệt độ cao! ESP32 Tổng: ${gwId} - Hiện tại: ${data.temperature.toFixed(2)}°C`);
    }
}

function showToast(message) {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.innerHTML = `<strong>Cảnh báo:</strong><br> ${message}`;
    container.appendChild(toast);
    
    // Tự động xóa sau 5 giây b
    setTimeout(() => { toast.remove(); }, 5000);
}