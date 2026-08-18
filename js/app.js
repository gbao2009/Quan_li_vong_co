/* ============================================================
   CONFIG
   ============================================================ */

const MQTT_URL =
  "wss://broker.hivemq.com:8884/mqtt";

let PROJECT_ID =
  localStorage.getItem(
    "farm_project_id_v3"
  ) ||
  "healthier_farm_demo_001";

let TOPIC_BASE = "";
let TOPIC_DATA = "";
let TOPIC_ALLOWED = "";
let TOPIC_STATUS = "";

const VALID_BPM_MIN = 40;
const VALID_BPM_MAX = 220;
const VALID_SPO2_MIN = 70;
const VALID_SPO2_MAX = 100;

/* Cấu hình biểu đồ thời gian thực:
   - CHART_WINDOW_MS: khoảng thời gian hiển thị trên trục X (cuộn theo thời gian thực)
   - SERIES_MAX_AGE_MS: thời gian tối đa giữ lại trong bộ nhớ (phục vụ phân tích/lịch sử gần đây)
   - SERIES_MAX_POINTS: giới hạn cứng số điểm/thiết bị để không phình bộ nhớ khi chạy nhiều giờ */
const CHART_WINDOW_MS = 60 * 1000;
const SERIES_MAX_AGE_MS = 5 * 60 * 1000;
const SERIES_MAX_POINTS = 1500;

/* Ngưỡng đánh giá "dữ liệu chậm" trên chấm trạng thái từng biểu đồ */
const DATA_SLOW_MS = 3000;

let client = null;
let allowedIds = [];
let devices = {};
let historyRows = [];
let selectedId = null;

/* ============================================================
   TOPICS
   ============================================================ */

function updateTopics(){

  TOPIC_BASE =
    "esp32_farm/" +
    PROJECT_ID;

  TOPIC_DATA =
    TOPIC_BASE +
    "/data";

  TOPIC_ALLOWED =
    TOPIC_BASE +
    "/allowed_ids";

  TOPIC_STATUS =
    TOPIC_BASE +
    "/status";

  document.getElementById(
    "sbProject"
  ).innerText =
    PROJECT_ID;

  document.getElementById(
    "settingsTopic"
  ).innerText =
    TOPIC_DATA;
}

/* ============================================================
   STORAGE
   ============================================================ */

function idsKey(){
  return "allowedIds_v4_" +
    PROJECT_ID;
}

function loadIds(){

  try{

    allowedIds =
      JSON.parse(
        localStorage.getItem(
          idsKey()
        ) || "[]"
      )
      .map(x =>
        String(x)
          .trim()
          .toUpperCase()
      )
      .filter(Boolean);

  }catch(e){

    allowedIds = [];
  }
}

function saveIds(){

  localStorage.setItem(
    idsKey(),
    JSON.stringify(
      allowedIds
    )
  );
}

/* ============================================================
   DEVICE MODEL
   ============================================================ */

function getDevice(id){

  id =
    String(id)
      .trim()
      .toUpperCase();

  if(!devices[id]){

    devices[id] = {

      id,

      online:false,
      lastSeen:0,

      latest:null,

      history:[],

      series:{
        t:[],
        bpm:[],
        spo2:[],
        ax:[],
        ay:[],
        az:[],
        da:[],
        gx:[],
        gy:[],
        gz:[],
        speed:[],
        distance:[]
      }
    };
  }

  return devices[id];
}

/* ============================================================
   MQTT JSON NORMALIZER
   ============================================================ */

function normalize(data){

  if(typeof data === "string"){

    const text =
      data.trim();

    if(
      text.startsWith("{")
    ){

      try{

        data =
          JSON.parse(
            text
          );

      }catch(e){

        return null;
      }

    }else{

      const obj = {};

      text
        .split(/[;\r\n]+/)
        .forEach(part => {

          const i =
            part.indexOf("=");

          if(i <= 0)
            return;

          const k =
            part
              .slice(0,i)
              .trim()
              .toUpperCase();

          const v =
            part
              .slice(i+1)
              .trim();

          obj[k] = v;
        });

      data = obj;
    }
  }

  if(!data)
    return null;

  const n =
    (v,d=0) => {

      const x =
        Number(v);

      return Number.isFinite(x)
        ? x
        : d;
    };

  return {

    device_id:String(
      data.device_id ??
      data.deviceId ??
      data.id ??
      data.D ??
      ""
    )
    .trim()
    .toUpperCase(),

    gateway_id:String(
      data.gateway_id ??
      data.gateway ??
      ""
    ),

    bpm:n(
      data.bpm ??
      data.B
    ),

    avg:n(
      data.avg ??
      data.A
    ),

    spo2:n(
      data.spo2 ??
      data.O
    ),

    ir:n(
      data.ir ??
      data.I
    ),

    red:n(
      data.red ??
      data.RED
    ),

    accelX:n(
      data.accelX ??
      data.X
    ),

    accelY:n(
      data.accelY ??
      data.Y
    ),

    accelZ:n(
      data.accelZ ??
      data.Z
    ),

    accelTotal:n(
      data.accelTotal ??
      data.AT
    ),

    dynamicAcceleration:n(
      data.dynamicAcceleration ??
      data.DA
    ),

    linearAccelX:n(
      data.linearAccelX ??
      data.LX
    ),

    linearAccelY:n(
      data.linearAccelY ??
      data.LY
    ),

    linearAccelZ:n(
      data.linearAccelZ ??
      data.LZ
    ),

    gyroX:n(
      data.gyroX ??
      data.GX
    ),

    gyroY:n(
      data.gyroY ??
      data.GY
    ),

    gyroZ:n(
      data.gyroZ ??
      data.GZ
    ),

    moving:n(
      data.moving ??
      data.M
    ),

    movementState:String(
      data.movementState ??
      data.S ??
      ""
    ).toUpperCase(),

    velocity:n(
      data.velocity ??
      data.V
    ),

    velocityKmh:n(
      data.velocityKmh ??
      data.VK
    ),

    distance:n(
      data.distance ??
      data.DIST
    )
  };
}

/* ============================================================
   HANDLE MQTT DATA
   ============================================================ */

function handleData(raw){

  const data =
    normalize(raw);

  if(
    !data ||
    !data.device_id
  )
    return;

  if(
    allowedIds.length &&
    !allowedIds.includes(
      data.device_id
    )
  )
    return;

  const d =
    getDevice(
      data.device_id
    );

  d.online = true;
  d.lastSeen = Date.now();
  d.gateway =
    data.gateway_id ||
    d.gateway ||
    "---";

  const movement =
    data.movementState ||
    (data.moving
      ? "MOVING"
      : "STILL");

  const activity =
    movement === "STILL" ? 0 :
    movement === "WALKING" ? 40 :
    movement === "MOVING" ? 65 :
    movement === "RUNNING" ? 90 :
    data.moving ? 60 : 0;

  const row = {

    time:
      new Date()
      .toLocaleString(
        "vi-VN"
      ),

    device_id:
      data.device_id,

    bpm:data.bpm,
    avg:data.avg,
    spo2:data.spo2,

    ir:data.ir,
    red:data.red,

    accelX:data.accelX,
    accelY:data.accelY,
    accelZ:data.accelZ,

    accelTotal:data.accelTotal,
    dynamicAcceleration:
      data.dynamicAcceleration,

    linearAccelX:
      data.linearAccelX,

    linearAccelY:
      data.linearAccelY,

    linearAccelZ:
      data.linearAccelZ,

    gyroX:data.gyroX,
    gyroY:data.gyroY,
    gyroZ:data.gyroZ,

    moving:data.moving,
    movementState:movement,

    velocity:data.velocity,
    velocityKmh:data.velocityKmh,
    distance:data.distance,

    activity,

    gateway:
      data.gateway_id ||
      "---"
  };

  row.valid =
    data.bpm >= VALID_BPM_MIN &&
    data.bpm <= VALID_BPM_MAX &&
    data.spo2 >= VALID_SPO2_MIN &&
    data.spo2 <= VALID_SPO2_MAX;

  row.warning =
    warningFor(row);

  d.latest = row;

  d.history.unshift(row);

  if(d.history.length > 200)
    d.history.pop();

  if(row.valid){

    historyRows.unshift(
      row
    );

    if(
      historyRows.length >
      500
    )
      historyRows.pop();

    const s =
      d.series;

    const t =
      Date.now();

    s.t.push(t);
    s.bpm.push(row.bpm);
    s.spo2.push(row.spo2);

    s.ax.push(row.accelX);
    s.ay.push(row.accelY);
    s.az.push(row.accelZ);
    s.da.push(row.dynamicAcceleration);

    s.gx.push(row.gyroX);
    s.gy.push(row.gyroY);
    s.gz.push(row.gyroZ);

    s.speed.push(
      row.velocityKmh
    );

    s.distance.push(
      row.distance
    );

    pruneSeries(s);
  }

  renderEverything();
}

/* ============================================================
   SERIES PRUNING (giới hạn bộ nhớ, không đụng tới dữ liệu gốc)
   ============================================================ */

function pruneSeries(s){

  const cutoff =
    Date.now() -
    SERIES_MAX_AGE_MS;

  while(
    s.t.length &&
    s.t[0] < cutoff
  ){

    Object.values(s)
      .forEach(arr =>
        arr.shift()
      );
  }

  while(
    s.t.length >
    SERIES_MAX_POINTS
  ){

    Object.values(s)
      .forEach(arr =>
        arr.shift()
      );
  }
}

/* ============================================================
   WARNING
   ============================================================ */

function warningFor(r){

  const a = [];

  if(r.bpm > 120)
    a.push(
      "Nhịp tim cao"
    );

  if(
    r.bpm > 0 &&
    r.bpm < 50
  )
    a.push(
      "Nhịp tim thấp"
    );

  if(
    r.spo2 > 0 &&
    r.spo2 < 95
  )
    a.push(
      "SpO2 thấp"
    );

  if(
    r.ir > 0 &&
    r.ir < 50000
  )
    a.push(
      "IR thấp"
    );

  if(
    r.movementState ===
    "RUNNING"
  )
    a.push(
      "Đang chạy"
    );

  return a.length
    ? a.join(", ")
    : "Bình thường";
}

/* ============================================================
   MQTT CONNECTION
   ============================================================ */

function connectMQTT(){

  updateTopics();

  if(client){

    try{
      client.end(
        true
      );
    }catch(e){}
  }

  client =
    mqtt.connect(
      MQTT_URL,
      {
        clientId:
          "WEB_" +
          Math.random()
            .toString(16)
            .slice(2),

        clean:true,

        reconnectPeriod:1000,

        connectTimeout:8000
      }
    );

  client.on(
    "connect",
    () => {

      setMqtt(
        "Đã kết nối",
        "ok"
      );

      client.subscribe(
        TOPIC_DATA
      );

      client.subscribe(
        TOPIC_STATUS
      );

      publishAllowed();

      toast(
        "MQTT đã kết nối"
      );
    }
  );

  client.on(
    "reconnect",
    () =>
      setMqtt(
        "Đang kết nối lại",
        "wait"
      )
  );

  client.on(
    "close",
    () =>
      setMqtt(
        "Mất kết nối",
        "bad"
      )
  );

  client.on(
    "error",
    () =>
      setMqtt(
        "Lỗi MQTT",
        "bad"
      )
  );

  client.on(
    "message",
    (
      topic,
      message
    ) => {

      const text =
        message.toString();

      if(
        topic ===
        TOPIC_DATA
      ){

        handleData(
          text
        );

      }else if(
        topic ===
        TOPIC_STATUS
      ){

        try{

          const status =
            JSON.parse(
              text
            );

          document.getElementById(
            "settingsGateway"
          ).innerText =
            status.gateway_id ||
            "---";

        }catch(e){}
      }
    }
  );
}

/* ============================================================
   MQTT STATUS
   ============================================================ */

function setMqtt(
  text,
  type
){

  const dot =
    type === "ok"
      ? "dot dot-ok"
      : type === "wait"
      ? "dot dot-warn"
      : "dot dot-bad";

  document.getElementById(
    "mqttText"
  ).innerText =
    text;

  document.getElementById(
    "sbMqtt"
  ).innerText =
    text;

  document.getElementById(
    "mqttDot"
  ).className =
    dot;

  document.getElementById(
    "sbDot"
  ).className =
    dot;
}

/* ============================================================
   ALLOWED IDS
   ============================================================ */

function publishAllowed(){

  if(
    !client ||
    !client.connected
  )
    return;

  client.publish(
    TOPIC_ALLOWED,
    allowedIds.join("\n"),
    {
      retain:true
    }
  );
}

function addId(){

  const id =
    document.getElementById(
      "newId"
    ).value
    .trim()
    .toUpperCase();

  if(!id){

    toast(
      "ID không được để trống"
    );

    return;
  }

  if(
    !allowedIds.includes(
      id
    )
  ){

    allowedIds.push(
      id
    );
  }

  saveIds();

  document.getElementById(
    "newId"
  ).value =
    "";

  publishAllowed();
  renderIds();

  toast(
    "Đã thêm " + id
  );
}

function deleteId(id){

  allowedIds =
    allowedIds.filter(
      x => x !== id
    );

  saveIds();

  publishAllowed();

  if(selectedId === id)
    selectedId = null;

  renderIds();
  renderEverything();
}

function clearIds(){

  if(
    !confirm(
      "Xóa toàn bộ ID?"
    )
  )
    return;

  allowedIds = [];

  saveIds();

  devices = {};
  historyRows = [];
  selectedId = null;

  publishAllowed();

  renderEverything();
}

function renderIds(){

  const tbody =
    document.getElementById(
      "idTable"
    );

  tbody.innerHTML =
    allowedIds.length
      ? allowedIds.map(
          id => {

            const d =
              devices[id];

            const status =
              d?.online
                ? "Trực tuyến"
                : d
                ? "Ngoại tuyến"
                : "Chưa có dữ liệu";

            const cls =
              d?.online
                ? "ok"
                : d
                ? "bad"
                : "warn";

            return `
              <tr>
                <td><b>${id}</b></td>
                <td class="${cls}">
                  ${status}
                </td>
                <td>
                  <button class="btn danger"
                    onclick="deleteId('${id}')">
                    Xóa
                  </button>
                </td>
              </tr>
            `;
          }
        ).join("")
      :
        `<tr><td colspan="3">Chưa có ID</td></tr>`;
}

/* ============================================================
   VIEW
   ============================================================ */

const titles = {

  dashboard:"Dashboard sức khỏe vật nuôi",

  devices:"Thiết bị",

  detail:"Theo dõi sức khỏe",

  analytics:"Phân tích dữ liệu",

  history:"Lịch sử",

  alerts:"Cảnh báo",

  settings:"Cài đặt MQTT"
};

function switchView(
  name
){

  document
    .querySelectorAll(
      ".view"
    )
    .forEach(v =>
      v.classList.remove(
        "active"
      )
    );

  document
    .getElementById(
      "view" +
      name.charAt(0)
        .toUpperCase() +
      name.slice(1)
    )
    .classList.add(
      "active"
    );

  document
    .querySelectorAll(
      ".nav-item"
    )
    .forEach(
      b =>
        b.classList.toggle(
          "active",
          b.dataset.view ===
          name
        )
    );

  document.getElementById(
    "pageTitle"
  ).innerText =
    titles[name] ||
    name;

  if(name === "analytics")
    renderAnalytics();

  if(name === "history")
    renderHistory();

  if(name === "alerts")
    renderAlerts();
}

function openSelected(){

  const id =
    selectedId ||
    primaryId();

  if(!id){

    switchView(
      "devices"
    );

    toast(
      "Chưa có thiết bị"
    );

    return;
  }

  selectedId =
    id;

  switchView(
    "detail"
  );

  renderDetail();
}

function toggleSidebar(){

  document
    .getElementById(
      "sidebar"
    )
    .classList.toggle(
      "open"
    );
}

function toggleTheme(){

  const html =
    document.documentElement;

  const next =
    html.dataset.theme ===
    "dark"
      ? "light"
      : "dark";

  html.dataset.theme =
    next;

  localStorage.setItem(
    "farm_theme_v2",
    next
  );

  document.getElementById(
    "themeBtn"
  ).innerText =
    next === "dark"
      ? "☀️"
      : "🌙";

  updateChartsTheme();
}

/* ============================================================
   PRIMARY DEVICE
   ============================================================ */

function primaryId(){

  if(
    selectedId &&
    devices[selectedId]
  )
    return selectedId;

  const online =
    allowedIds.find(
      id =>
        devices[id]?.online
    );

  if(online)
    return online;

  return allowedIds.find(
    id =>
      devices[id]
  ) || null;
}

/* ============================================================
   DASHBOARD RENDER
   ============================================================ */

function renderDashboard(){

  const id =
    primaryId();

  const d =
    id && devices[id];

  document.getElementById(
    "overviewId"
  ).innerText =
    id || "---";

  if(!d?.latest)
    return;

  const r =
    d.latest;

  document.getElementById(
    "kpiBpm"
  ).innerHTML =
    `${r.bpm}<span class="u">BPM</span>`;

  document.getElementById(
    "kpiAvg"
  ).innerText =
    r.avg;

  document.getElementById(
    "kpiSpo2"
  ).innerHTML =
    `${r.spo2}<span class="u">%</span>`;

  document.getElementById(
    "kpiSpo2Status"
  ).innerText =
    r.spo2 >= 95
      ? "Bình thường"
      : "Cảnh báo";

  document.getElementById(
    "kpiRed"
  ).innerText =
    r.red;

  document.getElementById(
    "kpiIr"
  ).innerText =
    r.ir;

  document.getElementById(
    "kpiSignal"
  ).innerText =
    r.ir >= 50000
      ? "Tín hiệu tốt"
      : r.ir >= 20000
      ? "Tín hiệu yếu"
      : "Không ổn định";

  document.getElementById(
    "kpiState"
  ).innerText =
    r.movementState;

  document.getElementById(
    "kpiMoving"
  ).innerText =
    r.moving;

  document.getElementById(
    "kpiGateway"
  ).innerText =
    d.gateway ||
    "---";

  document.getElementById(
    "kpiGatewayState"
  ).innerText =
    d.online
      ? "TRỰC TUYẾN"
      : "NGOẠI TUYẾN";

  document.getElementById(
    "dashSpeed"
  ).innerText =
    r.velocity.toFixed(3) +
    " m/s";

  document.getElementById(
    "dashSpeedKmh"
  ).innerText =
    r.velocityKmh.toFixed(3) +
    " km/h";

  document.getElementById(
    "dashDistance"
  ).innerText =
    r.distance.toFixed(3) +
    " m";

  document.getElementById(
    "dashMovement"
  ).innerText =
    r.movementState;

  const score =
    healthScore(
      r,
      d
    );

  document.getElementById(
    "scoreVal"
  ).innerText =
    score;

  document.getElementById(
    "scoreGauge"
  ).style.setProperty(
    "--pct",
    score
  );

  document.getElementById(
    "scoreText"
  ).innerText =
    score >= 75
      ? "Sức khỏe ổn định"
      : score >= 50
      ? "Cần theo dõi"
      : "Cần chú ý";

  renderFullTelemetry(
    "fullTelemetry",
    r
  );

  updateMainCharts(
    d
  );
}

/* ============================================================
   FULL TELEMETRY
   ============================================================ */

function renderFullTelemetry(
  target,
  r
){

  const el =
    document.getElementById(
      target
    );

  if(!el)
    return;

  const items = [

    ["RED", r.red],
    ["IR", r.ir],

    ["Gia tốc X", r.accelX.toFixed(3) + " m/s²"],
    ["Gia tốc Y", r.accelY.toFixed(3) + " m/s²"],
    ["Gia tốc Z", r.accelZ.toFixed(3) + " m/s²"],

    ["Tổng gia tốc", r.accelTotal.toFixed(3) + " m/s²"],
    ["Gia tốc động", r.dynamicAcceleration.toFixed(3) + " m/s²"],

    ["Gia tốc tuyến tính X", r.linearAccelX.toFixed(3) + " m/s²"],
    ["Gia tốc tuyến tính Y", r.linearAccelY.toFixed(3) + " m/s²"],
    ["Gia tốc tuyến tính Z", r.linearAccelZ.toFixed(3) + " m/s²"],

    ["Con quay X", r.gyroX.toFixed(2) + " °/s"],
    ["Con quay Y", r.gyroY.toFixed(2) + " °/s"],
    ["Con quay Z", r.gyroZ.toFixed(2) + " °/s"],

    ["Đang di chuyển", r.moving],
    ["Trạng thái", r.movementState],

    ["Tốc độ", r.velocity.toFixed(3) + " m/s"],
    ["Tốc độ (km/h)", r.velocityKmh.toFixed(3) + " km/h"],
    ["Quãng đường", r.distance.toFixed(3) + " m"]
  ];

  el.innerHTML =
    items.map(
      x =>
        `<div class="metric"><small>${x[0]}</small><b>${x[1]}</b></div>`
    ).join("");
}

/* ============================================================
   HEALTH SCORE
   ============================================================ */

function healthScore(
  r,
  d
){

  let s = 0;
  let c = 0;

  function add(
    value,
    good
  ){

    if(value){

      s += good;
      c++;
    }
  }

  add(
    r.bpm >= 60 &&
    r.bpm <= 110,
    25
  );

  add(
    r.spo2 >= 95,
    25
  );

  add(
    r.ir >= 50000,
    15
  );

  add(
    Math.abs(
      r.accelTotal - 9.80665
    ) < .25,
    10
  );

  add(
    r.movementState !==
    "RUNNING",
    10
  );

  add(
    d.online,
    15
  );

  return Math.round(
    Math.max(
      0,
      Math.min(
        100,
        s
      )
    )
  );
}

/* ============================================================
   CHARTS
   ============================================================ */

let bpmChart;
let spo2Chart;
let accChart;
let gyroChart;
let detailBpmChart;
let detailSpo2Chart;

/* ============================================================
   TIỆN ÍCH VẼ BIỂU ĐỒ: gradient fill, đổ bóng nhẹ, tooltip chi tiết
   Chỉ ảnh hưởng lớp HIỂN THỊ — không đụng tới dữ liệu / timestamp gốc
   ============================================================ */

function hexA(hex, alphaHex){
  return (hex || "#888888") + alphaHex;
}

/* Gradient dọc dưới đường biểu đồ: đậm gần đường, nhạt dần xuống dưới */
function gradientFill(colorHex){

  return (context) => {

    const {chart} = context;
    const {ctx, chartArea} = chart;

    if(!chartArea)
      return hexA(colorHex, "22");

    const g =
      ctx.createLinearGradient(
        0, chartArea.top,
        0, chartArea.bottom
      );

    g.addColorStop(0, hexA(colorHex, "99"));
    g.addColorStop(0.55, hexA(colorHex, "22"));
    g.addColorStop(1, hexA(colorHex, "00"));

    return g;
  };
}

/* Điểm dữ liệu mới nhất được phóng to nhẹ để tạo hiệu ứng "glow" —
   các điểm còn lại ẩn đi (radius 0) để đường luôn mượt và nhẹ CPU */
function lastPointRadius(baseline){

  return (context) => {

    const arr =
      context.dataset.data;

    let last =
      arr.length - 1;

    while(
      last >= 0 &&
      (arr[last] == null ||
       arr[last].y == null)
    )
      last--;

    return context.dataIndex === last
      ? baseline
      : 0;
  };
}

function makeDataset(label, colorHex, unit){

  return {
    label,
    data:[],
    unit,
    borderColor:colorHex,
    backgroundColor:gradientFill(colorHex),
    fill:true,
    borderWidth:2.5,
    tension:.35,
    cubicInterpolationMode:"monotone",
    spanGaps:false,
    pointRadius:lastPointRadius(4.5),
    pointHoverRadius:5.5,
    pointBackgroundColor:colorHex,
    pointBorderColor:"#fff",
    pointBorderWidth:1.5
  };
}

/* Plugin: đổ bóng (drop-shadow) nhẹ cho đường + điểm biểu đồ */
const shadowLinePlugin = {
  id:"shadowLine",
  beforeDatasetsDraw(chart){
    const {ctx} = chart;
    ctx.save();
    ctx.shadowColor = "rgba(20,30,60,.22)";
    ctx.shadowBlur = 7;
    ctx.shadowOffsetY = 3;
  },
  afterDatasetsDraw(chart){
    chart.ctx.restore();
  }
};

if(
  typeof Chart !==
  "undefined" &&
  !Chart.__shadowLineRegistered
){
  Chart.register(shadowLinePlugin);
  Chart.__shadowLineRegistered = true;
}

function chartOptions(){

  return {

    responsive:true,
    maintainAspectRatio:false,
    animation:false,
    normalized:true,

    interaction:{
      mode:"index",
      intersect:false
    },

    plugins:{

      legend:{
        position:"bottom",
        labels:{
          usePointStyle:true,
          boxHeight:6,
          color:css("--text-2"),
          font:{family:"'Inter',sans-serif", size:11.5}
        }
      },

      tooltip:{
        backgroundColor:css("--surface"),
        titleColor:css("--text-1"),
        bodyColor:css("--text-2"),
        borderColor:css("--border"),
        borderWidth:1,
        padding:10,
        cornerRadius:10,
        displayColors:true,
        titleFont:{family:"'JetBrains Mono',monospace", size:12, weight:"600"},
        bodyFont:{family:"'JetBrains Mono',monospace", size:12},

        callbacks:{

          title(items){

            if(!items.length)
              return "";

            const v =
              items[0].parsed.x;

            return "Thời gian: " +
              new Date(v)
                .toLocaleTimeString(
                  "vi-VN",
                  {hour12:false}
                );
          },

          label(item){

            const ds =
              item.dataset;

            const val =
              item.parsed.y;

            if(val === null || val === undefined)
              return ds.label + ": mất dữ liệu";

            const unit =
              ds.unit
                ? " " + ds.unit
                : "";

            return ds.label + ": " +
              (typeof val === "number"
                ? val.toFixed(2)
                : val) +
              unit;
          }
        }
      }
    },

    scales:{

      x:{
        type:"time",
        time:{
          unit:"second",
          displayFormats:{
            second:"HH:mm:ss",
            minute:"HH:mm"
          }
        },

        ticks:{
          color:css("--text-3"),
          maxTicksLimit:6,
          autoSkip:true,
          font:{family:"'JetBrains Mono',monospace", size:10.5}
        },

        grid:{
          color:css("--border"),
          tickLength:0
        }
      },

      y:{
        ticks:{
          color:css("--text-3"),
          font:{family:"'JetBrains Mono',monospace", size:10.5}
        },

        grid:{
          color:css("--border")
        }
      }
    },

    elements:{
      point:{
        radius:0,
        hitRadius:6
      },

      line:{
        tension:.35,
        borderWidth:2.5
      }
    }
  };
}

/* ============================================================
   TRẠNG THÁI DỮ LIỆU TRỰC TIẾP (chấm xanh/vàng/đỏ trên mỗi biểu đồ)
   ============================================================ */

function dataStatus(d){

  if(!d || !d.latest)
    return {
      cls:"ds-wait",
      dot:"dot dot-warn",
      text:"Đang chờ dữ liệu"
    };

  if(!d.online)
    return {
      cls:"ds-lost",
      dot:"dot dot-bad",
      text:"Mất kết nối"
    };

  if(
    Date.now() - d.lastSeen >
    DATA_SLOW_MS
  )
    return {
      cls:"ds-slow",
      dot:"dot dot-warn",
      text:"Dữ liệu chậm"
    };

  return {
    cls:"ds-live",
    dot:"dot dot-ok dot-live-pulse",
    text:"Đang nhận dữ liệu"
  };
}

function statusChipHTML(d){

  const s =
    dataStatus(d);

  return `<span class="data-status ${s.cls}"><span class="${s.dot}"></span>${s.text}</span>`;
}

function renderChartStatuses(){

  const id =
    primaryId();

  const d =
    id && devices[id];

  ["statBpm","statSpo2","statAcc","statGyro"]
    .forEach(elId => {

      const el =
        document.getElementById(
          elId
        );

      if(el)
        el.innerHTML =
          statusChipHTML(d);
    });

  const dd =
    selectedId &&
    devices[selectedId];

  ["statDetailBpm","statDetailSpo2"]
    .forEach(elId => {

      const el =
        document.getElementById(
          elId
        );

      if(el)
        el.innerHTML =
          statusChipHTML(dd);
    });
}

/* Chuyển series {t:[...], v:[...]} thành mảng {x,y} thời gian thực
   cho trục thời gian của Chart.js — không nội suy, không tạo điểm giả */
function toXY(tArr, vArr){

  const out = [];

  for(let i=0; i<tArr.length; i++){

    out.push({
      x:tArr[i],
      y:vArr[i] === undefined
        ? null
        : vArr[i]
    });
  }

  return out;
}

/* Cuộn cửa sổ hiển thị (trục X) theo đồng hồ thực, không dùng
   animation frame để giả lập — chỉ dịch biên min/max theo Date.now() */
function slideChartWindow(chart){

  if(!chart)
    return;

  const now =
    Date.now();

  chart.options.scales.x.min =
    now - CHART_WINDOW_MS;

  chart.options.scales.x.max =
    now;
}

function slideAllChartWindows(){

  [
    bpmChart,
    spo2Chart,
    accChart,
    gyroChart,
    detailBpmChart,
    detailSpo2Chart
  ].forEach(ch => {

    if(!ch)
      return;

    if(
      ch.canvas &&
      ch.canvas.offsetParent === null
    )
      return;

    slideChartWindow(ch);
    ch.update("none");
  });
}

function initCharts(){

  bpmChart =
    new Chart(
      document.getElementById(
        "bpmChart"
      ),
      {
        type:"line",

        data:{
          datasets:[
            makeDataset("Nhịp tim", css("--accent"), "BPM")
          ]
        },

        options:chartOptions()
      }
    );

  spo2Chart =
    new Chart(
      document.getElementById(
        "spo2Chart"
      ),
      {
        type:"line",

        data:{
          datasets:[
            makeDataset("SpO2", css("--cyan"), "%")
          ]
        },

        options:chartOptions()
      }
    );

  accChart =
    new Chart(
      document.getElementById(
        "accChart"
      ),
      {
        type:"line",

        data:{

          datasets:[
            makeDataset("Trục X", css("--accent"), "m/s²"),
            makeDataset("Trục Y", css("--cyan"), "m/s²"),
            makeDataset("Trục Z", css("--warn"), "m/s²"),
            makeDataset("Gia tốc động", css("--ok"), "m/s²")
          ]
        },

        options:chartOptions()
      }
    );

  gyroChart =
    new Chart(
      document.getElementById(
        "gyroChart"
      ),
      {
        type:"line",

        data:{

          datasets:[
            makeDataset("Trục X", css("--accent"), "°/s"),
            makeDataset("Trục Y", css("--cyan"), "°/s"),
            makeDataset("Trục Z", css("--warn"), "°/s")
          ]
        },

        options:chartOptions()
      }
    );

  detailBpmChart =
    new Chart(
      document.getElementById(
        "detailBpm"
      ),
      {
        type:"line",
        data:{
          datasets:[
            makeDataset("Nhịp tim", css("--accent"), "BPM")
          ]
        },
        options:chartOptions()
      }
    );

  detailSpo2Chart =
    new Chart(
      document.getElementById(
        "detailSpo2"
      ),
      {
        type:"line",
        data:{
          datasets:[
            makeDataset("SpO2", css("--cyan"), "%")
          ]
        },
        options:chartOptions()
      }
    );
}

function updateMainCharts(
  d
){

  const s =
    d.series;

  bpmChart.data.datasets[0].data =
    toXY(s.t, s.bpm);

  slideChartWindow(bpmChart);
  bpmChart.update("none");

  spo2Chart.data.datasets[0].data =
    toXY(s.t, s.spo2);

  slideChartWindow(spo2Chart);
  spo2Chart.update("none");

  accChart.data.datasets[0].data =
    toXY(s.t, s.ax);

  accChart.data.datasets[1].data =
    toXY(s.t, s.ay);

  accChart.data.datasets[2].data =
    toXY(s.t, s.az);

  accChart.data.datasets[3].data =
    toXY(s.t, s.da);

  slideChartWindow(accChart);
  accChart.update("none");

  gyroChart.data.datasets[0].data =
    toXY(s.t, s.gx);

  gyroChart.data.datasets[1].data =
    toXY(s.t, s.gy);

  gyroChart.data.datasets[2].data =
    toXY(s.t, s.gz);

  slideChartWindow(gyroChart);
  gyroChart.update("none");

  renderChartStatuses();
}

/* ============================================================
   DEVICE CARDS
   ============================================================ */

function renderDevices(){

  const box =
    document.getElementById(
      "deviceList"
    );

  const ids =
    allowedIds.filter(
      id =>
        devices[id]
    );

  if(!ids.length){

    box.innerHTML =
      `<div class="empty">Chưa có dữ liệu thiết bị</div>`;

    return;
  }

  box.innerHTML =
    ids.map(
      id => {

        const d =
          devices[id];

        const r =
          d.latest;

        if(!r)
          return "";

        return `
          <div class="device-card">

            <div class="dc-top">
              <span class="dc-id">
                🐕 ${id}
              </span>

              <span class="dc-badge ${
                d.online
                  ? "online"
                  : "offline"
              }">
                ● ${
                  d.online
                    ? "Trực tuyến"
                    : "Ngoại tuyến"
                }
              </span>
            </div>

            <div class="dc-stats">

              <div class="dc-stat">
                <span>❤️ BPM</span>
                <b>${r.bpm}</b>
              </div>

              <div class="dc-stat">
                <span>🫁 SpO2</span>
                <b>${r.spo2}%</b>
              </div>

              <div class="dc-stat">
                <span>🐾 Trạng thái</span>
                <b>${r.movementState}</b>
              </div>

              <div class="dc-stat">
                <span>⚡ Tốc độ</span>
                <b>${r.velocityKmh.toFixed(2)} km/h</b>
              </div>

            </div>

            <div class="panel-sub">
              DA=${r.dynamicAcceleration.toFixed(3)} m/s²
              · DIST=${r.distance.toFixed(2)} m
            </div>

            <button
              class="btn primary"
              style="width:100%;margin-top:12px"
              onclick="selectDevice('${id}')"
            >
              Xem chi tiết
            </button>

          </div>
        `;
      }
    ).join("");
}

function selectDevice(
  id
){

  selectedId =
    id;

  switchView(
    "detail"
  );

  renderDetail();
}

/* ============================================================
   DETAIL
   ============================================================ */

function renderDetail(){

  const d =
    devices[selectedId];

  if(!d?.latest)
    return;

  const r =
    d.latest;

  document.getElementById(
    "detailId"
  ).innerText =
    selectedId;

  document.getElementById(
    "dBpm"
  ).innerText =
    r.bpm;

  document.getElementById(
    "dAvg"
  ).innerText =
    r.avg;

  document.getElementById(
    "dSpo2"
  ).innerText =
    r.spo2 + "%";

  document.getElementById(
    "dRed"
  ).innerText =
    r.red;

  document.getElementById(
    "dIr"
  ).innerText =
    r.ir;

  document.getElementById(
    "dState"
  ).innerText =
    r.movementState;

  renderFullTelemetry(
    "detailTelemetry",
    r
  );

  const s =
    d.series;

  detailBpmChart.data.datasets[0].data =
    toXY(s.t, s.bpm);

  slideChartWindow(detailBpmChart);
  detailBpmChart.update("none");

  detailSpo2Chart.data.datasets[0].data =
    toXY(s.t, s.spo2);

  slideChartWindow(detailSpo2Chart);
  detailSpo2Chart.update("none");

  renderChartStatuses();

  const tbody =
    document.getElementById(
      "detailHistory"
    );

  tbody.innerHTML =
    d.history
      .slice(0,50)
      .map(
        r => `
          <tr>
            <td>${r.time}</td>
            <td>${r.bpm}</td>
            <td>${r.avg}</td>
            <td>${r.spo2}</td>
            <td>${r.ir}</td>
            <td>${r.red}</td>
            <td>${r.accelX.toFixed(3)}</td>
            <td>${r.accelY.toFixed(3)}</td>
            <td>${r.accelZ.toFixed(3)}</td>
            <td>${r.dynamicAcceleration.toFixed(3)}</td>
            <td>${r.movementState}</td>
            <td>${r.velocityKmh.toFixed(3)}</td>
          </tr>
        `
      ).join("");
}

/* ============================================================
   ANALYTICS
   ============================================================ */

function populateAnalytics(){

  const sel =
    document.getElementById(
      "analyticsDevice"
    );

  const old =
    sel.value;

  sel.innerHTML =
    allowedIds
      .filter(
        id =>
          devices[id]
      )
      .map(
        id =>
          `<option value="${id}">${id}</option>`
      )
      .join("");

  if(
    old &&
    devices[old]
  )
    sel.value =
      old;

  if(
    !sel.value
  )
    sel.value =
      primaryId() || "";
}

function statHTML(
  arr
){

  if(!arr.length)
    return `<div class="metric"><small>Dữ liệu</small><b>---</b></div>`;

  const avg =
    arr.reduce(
      (a,b)=>a+b,
      0
    ) / arr.length;

  return `
    <div class="metric"><small>Trung bình</small><b>${avg.toFixed(2)}</b></div>
    <div class="metric"><small>Nhỏ nhất</small><b>${Math.min(...arr).toFixed(2)}</b></div>
    <div class="metric"><small>Lớn nhất</small><b>${Math.max(...arr).toFixed(2)}</b></div>
  `;
}

function renderAnalytics(){

  populateAnalytics();

  const id =
    document.getElementById(
      "analyticsDevice"
    ).value;

  const d =
    devices[id];

  if(!d)
    return;

  const h =
    d.history.filter(
      r =>
        r.valid
    );

  const bpm =
    h.map(r=>r.bpm);

  const spo2 =
    h.map(r=>r.spo2);

  const da =
    h.map(
      r=>r.dynamicAcceleration
    );

  const speed =
    h.map(
      r=>r.velocityKmh
    );

  document.getElementById(
    "anaBpm"
  ).innerHTML =
    statHTML(
      bpm
    );

  document.getElementById(
    "anaSpo2"
  ).innerHTML =
    statHTML(
      spo2
    );

  document.getElementById(
    "anaAcc"
  ).innerHTML =
    statHTML(
      da
    );

  document.getElementById(
    "anaVelocity"
  ).innerHTML =
    statHTML(
      speed
    ) +
    `
    <div class="metric">
      <small>Quãng đường hiện tại</small>
      <b>${(d.latest?.distance||0).toFixed(2)} m</b>
    </div>
    `;
}

/* ============================================================
   HISTORY
   ============================================================ */

function renderHistory(){

  const search =
    document.getElementById(
      "historySearch"
    ).value
      .trim()
      .toUpperCase();

  const filter =
    document.getElementById(
      "historyDevice"
    ).value;

  const rows =
    historyRows.filter(
      r =>
        (!search ||
         r.device_id.includes(
           search
         )) &&
        (!filter ||
         r.device_id ===
         filter)
    );

  document.getElementById(
    "historyTable"
  ).innerHTML =
    rows
      .slice(0,200)
      .map(
        r => `
          <tr>
            <td>${r.time}</td>
            <td>${r.device_id}</td>
            <td>${r.bpm}</td>
            <td>${r.avg}</td>
            <td>${r.spo2}</td>
            <td>${r.ir}</td>
            <td>${r.red}</td>
            <td>${r.accelX.toFixed(3)}</td>
            <td>${r.accelY.toFixed(3)}</td>
            <td>${r.accelZ.toFixed(3)}</td>
            <td>${r.dynamicAcceleration.toFixed(3)}</td>
            <td>${r.movementState}</td>
            <td>${r.velocityKmh.toFixed(3)}</td>
            <td>${r.distance.toFixed(3)}</td>
          </tr>
        `
      ).join("") ||
      `<tr><td colspan="14">Chưa có dữ liệu</td></tr>`;
}

/* ============================================================
   ALERTS
   ============================================================ */

function renderAlerts(){

  const box =
    document.getElementById(
      "alertList"
    );

  const alerts = [];

  allowedIds.forEach(
    id => {

      const d =
        devices[id];

      if(!d?.latest)
        return;

      const r =
        d.latest;

      if(
        r.bpm > 120
      )
        alerts.push(
          ["🔴","critical",id,"Nhịp tim cao: "+r.bpm+" BPM"]
        );

      if(
        r.bpm > 0 &&
        r.bpm < 50
      )
        alerts.push(
          ["🔴","critical",id,"Nhịp tim thấp: "+r.bpm+" BPM"]
        );

      if(
        r.spo2 > 0 &&
        r.spo2 < 95
      )
        alerts.push(
          ["🟡","warning",id,"SpO2 thấp: "+r.spo2+"%"]
        );

      if(
        r.ir > 0 &&
        r.ir < 50000
      )
        alerts.push(
          ["🟡","warning",id,"IR thấp: "+r.ir]
        );

      if(
        r.movementState ===
        "RUNNING"
      )
        alerts.push(
          ["🔵","info",id,"Thiết bị đang RUNNING"]
        );
    }
  );

  box.innerHTML =
    alerts.length
      ? alerts.map(
          a =>
            `<div class="alert-row">
              <div class="alert-ic ${
                a[1]==="critical"
                  ? "icon-red"
                  : a[1]==="warning"
                  ? "icon-amber"
                  : "icon-blue"
              }">${a[0]}</div>
              <div class="alert-text">
                <b>${a[2]}</b>
                <span>${a[3]}</span>
              </div>
            </div>`
        ).join("")
      : `<div class="empty">Chưa có cảnh báo</div>`;
}

/* ============================================================
   EXPORT
   ============================================================ */

function exportCSV(){

  if(
    !historyRows.length
  ){

    toast(
      "Chưa có dữ liệu"
    );

    return;
  }

  const headers = [
    "Time","Device","BPM","AVG","SpO2","IR","RED",
    "AccelX","AccelY","AccelZ","DynamicAcc",
    "LinearX","LinearY","LinearZ",
    "GyroX","GyroY","GyroZ",
    "Moving","MovementState",
    "Velocity","VelocityKmh","Distance"
  ];

  const lines =
    [
      headers.join(",")
    ];

  historyRows.forEach(
    r => {

      lines.push(
        [
          `"${r.time}"`,
          r.device_id,
          r.bpm,
          r.avg,
          r.spo2,
          r.ir,
          r.red,
          r.accelX,
          r.accelY,
          r.accelZ,
          r.dynamicAcceleration,
          r.linearAccelX,
          r.linearAccelY,
          r.linearAccelZ,
          r.gyroX,
          r.gyroY,
          r.gyroZ,
          r.moving,
          r.movementState,
          r.velocity,
          r.velocityKmh,
          r.distance
        ].join(",")
      );
    }
  );

  const blob =
    new Blob(
      [lines.join("\n")],
      {
        type:
          "text/csv;charset=utf-8"
      }
    );

  const url =
    URL.createObjectURL(
      blob
    );

  const a =
    document.createElement(
      "a"
    );

  a.href = url;

  a.download =
    "pet_health_" +
    PROJECT_ID +
    ".csv";

  a.click();

  URL.revokeObjectURL(
    url
  );
}

/* ============================================================
   PROJECT
   ============================================================ */

function saveProject(){

  const p =
    document.getElementById(
      "projectInput"
    ).value.trim();

  if(!p)
    return;

  PROJECT_ID =
    p;

  localStorage.setItem(
    "farm_project_id_v3",
    PROJECT_ID
  );

  allowedIds = [];
  devices = {};
  historyRows = [];
  selectedId = null;

  saveIds();
  updateTopics();
  loadIds();

  connectMQTT();
  renderEverything();

  toast(
    "Đã đổi Project"
  );
}

/* ============================================================
   ONLINE TIMEOUT
   ============================================================ */

function checkTimeouts(){

  const now =
    Date.now();

  let changed =
    false;

  Object.values(
    devices
  ).forEach(
    d => {

      if(
        d.online &&
        now -
        d.lastSeen >
        10000
      ){

        d.online =
          false;

        changed = true;

        /* Mất kết nối: chèn một điểm null ngay sau mẫu thật cuối
           cùng để biểu đồ hiển thị khoảng trống rõ ràng, thay vì
           tự nối liền sang lần nhận dữ liệu tiếp theo. Đây KHÔNG
           phải dữ liệu giả — chỉ là dấu hiệu "không có dữ liệu". */
        const s =
          d.series;

        if(
          s.t.length &&
          s.bpm[s.bpm.length-1] !== null
        ){

          const gapT =
            Math.min(
              d.lastSeen + 1,
              now
            );

          Object.keys(s)
            .forEach(k => {

              s[k].push(
                k === "t"
                  ? gapT
                  : null
              );
            });

          pruneSeries(s);
        }
      }
    }
  );

  renderOnline();
  renderChartStatuses();

  if(changed)
    renderEverything();
}

/* ============================================================
   ONLINE
   ============================================================ */

function renderOnline(){

  const online =
    allowedIds.filter(
      id =>
        devices[id]?.online
    ).length;

  const text =
    online +
    "/" +
    allowedIds.length;

  document.getElementById(
    "sbOnline"
  ).innerText =
    text;

  document.getElementById(
    "tbOnline"
  ).innerText =
    text;
}

/* ============================================================
   REFRESH
   ============================================================ */

function renderEverything(){

  renderOnline();
  renderIds();
  renderDevices();
  renderDashboard();

  if(
    selectedId
  )
    renderDetail();

  renderAnalytics();
  renderHistory();
  renderAlerts();
}

function refreshUI(){
  renderEverything();
  toast(
    "Đã làm mới"
  );
}

/* ============================================================
   CSS VARIABLE / TOAST
   ============================================================ */

function css(name){

  return getComputedStyle(
    document.documentElement
  )
    .getPropertyValue(
      name
    )
    .trim();
}

function updateChartsTheme(){

  [
    bpmChart,
    spo2Chart,
    accChart,
    gyroChart,
    detailBpmChart,
    detailSpo2Chart
  ].forEach(
    ch => {

      if(!ch)
        return;

      Object.values(
        ch.options.scales || {}
      ).forEach(
        sc => {

          if(sc.grid)
            sc.grid.color =
              css("--border");

          if(sc.ticks)
            sc.ticks.color =
              css("--text-3");
        }
      );

      ch.update("none");
    }
  );
}

let toastTimer;

function toast(
  text
){

  const el =
    document.getElementById(
      "toast"
    );

  el.innerText =
    text;

  el.classList.add(
    "show"
  );

  clearTimeout(
    toastTimer
  );

  toastTimer =
    setTimeout(
      () =>
        el.classList.remove(
          "show"
        ),
      2200
    );
}

/* ============================================================
   INITIALIZE
   ============================================================ */

(function init(){

  const theme =
    localStorage.getItem(
      "farm_theme_v2"
    ) ||
    "light";

  document.documentElement
    .dataset.theme =
    theme;

  document.getElementById(
    "themeBtn"
  ).innerText =
    theme === "dark"
      ? "☀️"
      : "🌙";

  document.getElementById(
    "projectInput"
  ).value =
    PROJECT_ID;

  updateTopics();
  loadIds();
  renderIds();
  initCharts();
  connectMQTT();
  renderEverything();

  setInterval(
    checkTimeouts,
    1000
  );

  setInterval(
    () => {

      document.getElementById(
        "tbTime"
      ).innerText =
        new Date()
          .toLocaleTimeString(
            "vi-VN",
            {
              hour12:false
            }
          );

      renderOnline();
      renderChartStatuses();
      slideAllChartWindows();

    },
    1000
  );

})();
