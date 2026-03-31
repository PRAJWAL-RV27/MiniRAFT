const canvas = document.getElementById("board");
const ctx = canvas.getContext("2d");
const statusText = document.getElementById("status");
const clearBoardButton = document.getElementById("clearBoard");

function resizeCanvas() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight - 60;
}

resizeCanvas();
window.addEventListener("resize", resizeCanvas);

let drawing = false;
let lastPoint = null;

const wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
const socket = new WebSocket(`${wsProtocol}://${window.location.host}`);

socket.onopen = ()=>{
    statusText.innerText = "Connected to gateway";
    statusText.style.background = "green";
};

socket.onclose = ()=>{
    statusText.innerText = "Gateway disconnected";
    statusText.style.background = "red";
};

socket.onmessage = (event)=>{

    const data = JSON.parse(event.data);

    if (data.type !== "system") {
        statusText.innerText = "Connected to gateway";
        statusText.style.background = "green";
    }

    if(data.type === "stroke"){
        drawStroke(data.stroke);
    }

    if(data.type === "clear"){
        clearBoard();
    }

    if(data.type === "snapshot"){
        renderSnapshot(data.entries || []);
    }

    if(data.type === "system") {
        statusText.innerText = data.message || "System message";
        statusText.style.background = data.level === "warning" ? "#d97706" : "#dc2626";

        if (data.level === "warning") {
            setTimeout(() => {
                if (socket.readyState === WebSocket.OPEN) {
                    statusText.innerText = "Connected to gateway";
                    statusText.style.background = "green";
                }
            }, 1800);
        }
    }
};

clearBoardButton.addEventListener("click", ()=>{
    sendCommand({ type: "clear" });
});

canvas.addEventListener("mousedown", (e)=>startDrawing(e.clientX, e.clientY));
canvas.addEventListener("mouseup", stopDrawing);
canvas.addEventListener("mouseleave", stopDrawing);

canvas.addEventListener("touchstart", (e)=>{
    const touch = e.touches[0];
    if (!touch) return;
    e.preventDefault();
    startDrawing(touch.clientX, touch.clientY);
}, { passive: false });

canvas.addEventListener("touchend", stopDrawing);
canvas.addEventListener("touchcancel", stopDrawing);

canvas.addEventListener("mousemove",(e)=>{

    sendStrokeFromPointer(e.clientX, e.clientY);
});

canvas.addEventListener("touchmove",(e)=>{
    const touch = e.touches[0];
    if (!touch) return;
    e.preventDefault();
    sendStrokeFromPointer(touch.clientX, touch.clientY);
}, { passive: false });

function clearBoard() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function renderSnapshot(entries) {
    clearBoard();

    for (const entry of entries) {
        if (entry.action === "clear") {
            clearBoard();
            continue;
        }

        if (entry.stroke) {
            drawStroke(entry.stroke);
        }
    }
}

function startDrawing(clientX, clientY) {
    drawing = true;
    const rect = canvas.getBoundingClientRect();
    lastPoint = {
        x: clientX - rect.left,
        y: clientY - rect.top
    };
}

function stopDrawing() {
    drawing = false;
    lastPoint = null;
}

function sendStrokeFromPointer(clientX, clientY) {
    if(!drawing || !lastPoint) return;

    const rect = canvas.getBoundingClientRect();
    const nextPoint = {
        x: clientX - rect.left,
        y: clientY - rect.top
    };

    const stroke = {
        fromX:lastPoint.x,
        fromY:lastPoint.y,
        toX:nextPoint.x,
        toY:nextPoint.y,
        color:"#0f172a",
        width:3
    };

    lastPoint = nextPoint;

    sendCommand({
        type:"stroke",
        stroke:stroke
    });
}

function sendCommand(command) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(command));
    }
}

function drawStroke(stroke){

    ctx.beginPath();
    ctx.moveTo(stroke.fromX, stroke.fromY);
    ctx.lineTo(stroke.toX, stroke.toY);
    ctx.strokeStyle = stroke.color || "#222";
    ctx.lineWidth = stroke.width || 2;
    ctx.lineCap = "round";
    ctx.stroke();
}