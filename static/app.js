/* ─────────────────────────────────────────────────────────────────
   CATA — Voice Chatbot  |  app.js  (streaming version)

   Pipeline:
     Mic ──► /api/stt ──► transcript shown in input
        ──► /api/chat/stream (SSE)
              ├─ {type:"text",  delta}  → append word-by-word to bubble
              ├─ {type:"audio", data}   → decode base64 WAV → queue playback
              └─ {type:"done"}          → finalise
   ───────────────────────────────────────────────────────────────── */

// ── DOM refs ──────────────────────────────────────────────────────
const messagesEl = document.getElementById('messages');
const textInput = document.getElementById('textInput');
const sendBtn = document.getElementById('sendBtn');
const micBtn = document.getElementById('micBtn');
const micIcon = document.getElementById('micIcon');
const stopIcon = document.getElementById('stopIcon');
const voiceSelect = document.getElementById('voiceSelect');
const clearBtn = document.getElementById('clearBtn');
const statusDot = document.querySelector('#statusDot .dot');
const statusText = document.getElementById('statusText');
const recordingLabel = document.getElementById('recordingLabel');
const speedPills = document.querySelectorAll('.speed-pill');

// ── State ─────────────────────────────────────────────────────────
let conversationHistory = [];
let mediaRecorder = null;
let audioChunks = [];
let isRecording = false;
let selectedSpeed = 'normal';
let selectedVoiceId = 'a0e99841-438c-4a64-b679-ae501e7d6091';

// ── Audio queue (sequential playback of WAV chunks) ───────────────
const audioQueue = [];
let audioPlaying = false;
let currentSource = null; // keep ref to stop on interrupt
let audioCtx = null; // single shared AudioContext

function getAudioCtx() {
    if (!audioCtx || audioCtx.state === 'closed') {
        audioCtx = new(window.AudioContext || window.webkitAudioContext)();
    }
    return audioCtx;
}

function stopAllAudio() {
    if (currentSource) {
        try { currentSource.stop(); } catch (_) {}
        currentSource = null;
    }
    audioQueue.length = 0;
    audioPlaying = false;
}

async function enqueueAudio(base64wav) {
    audioQueue.push(base64wav);
    if (!audioPlaying) drainQueue();
}

async function drainQueue() {
    if (audioPlaying || audioQueue.length === 0) return;
    audioPlaying = true;

    while (audioQueue.length > 0) {
        const b64 = audioQueue.shift();
        try {
            const binary = atob(b64);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);

            const ctx = getAudioCtx();
            const decoded = await ctx.decodeAudioData(bytes.buffer);
            await playBuffer(ctx, decoded);
        } catch (err) {
            console.error('Audio decode/play error:', err);
        }
    }

    audioPlaying = false;
    setStatus('ready', 'Ready');
}

function playBuffer(ctx, buffer) {
    return new Promise(resolve => {
        const src = ctx.createBufferSource();
        src.buffer = buffer;
        src.connect(ctx.destination);
        currentSource = src;
        src.onended = () => {
            currentSource = null;
            resolve();
        };
        src.start(0);
    });
}

// ── Status ────────────────────────────────────────────────────────
function setStatus(state, label) {
    statusDot.className = `dot ${state}`;
    statusText.textContent = label;
}

// ── Voices ────────────────────────────────────────────────────────
async function loadVoices() {
    try {
        const res = await fetch('/api/voices');
        const data = await res.json();
        voiceSelect.innerHTML = '';
        (data.voices || []).forEach(v => {
            const opt = document.createElement('option');
            opt.value = v.id;
            opt.textContent = v.name + (v.gender ? ` (${v.gender})` : '');
            if (v.id === selectedVoiceId) opt.selected = true;
            voiceSelect.appendChild(opt);
        });
        if (voiceSelect.options.length) selectedVoiceId = voiceSelect.value;
    } catch (err) {
        console.error('Voice load failed:', err);
        voiceSelect.innerHTML = '<option value="">Default voice</option>';
    }
}
voiceSelect.addEventListener('change', () => { selectedVoiceId = voiceSelect.value; });

// ── Speed pills ───────────────────────────────────────────────────
speedPills.forEach(pill => {
    pill.addEventListener('click', () => {
        speedPills.forEach(p => p.classList.remove('active'));
        pill.classList.add('active');
        selectedSpeed = pill.dataset.speed;
    });
});

// ── Textarea auto-resize ──────────────────────────────────────────
textInput.addEventListener('input', () => {
    textInput.style.height = 'auto';
    textInput.style.height = Math.min(textInput.scrollHeight, 140) + 'px';
});
textInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSendText();
    }
});
sendBtn.addEventListener('click', handleSendText);
clearBtn.addEventListener('click', clearChat);

// ── Bubble helpers ────────────────────────────────────────────────
function removeWelcomeHint() {
    const h = messagesEl.querySelector('.welcome-hint');
    if (h) h.remove();
}

function appendMessage(role, text) {
    removeWelcomeHint();
    const row = document.createElement('div');
    row.className = `message-row ${role}`;

    const avatar = document.createElement('div');
    avatar.className = 'avatar';
    avatar.textContent = role === 'user' ? 'U' : 'AI';

    const bubble = document.createElement('div');
    bubble.className = 'bubble';
    bubble.textContent = text;

    row.appendChild(avatar);
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    scrollToBottom();
    return bubble;
}

function createStreamingBubble() {
    removeWelcomeHint();
    const row = document.createElement('div');
    row.className = 'message-row ai';

    const avatar = document.createElement('div');
    avatar.className = 'avatar';
    avatar.textContent = 'AI';

    const bubble = document.createElement('div');
    bubble.className = 'bubble streaming';
    bubble.textContent = '';

    row.appendChild(avatar);
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    scrollToBottom();
    return bubble;
}

function showTyping() {
    removeWelcomeHint();
    const row = document.createElement('div');
    row.className = 'message-row ai';
    row.id = 'typingIndicator';

    const avatar = document.createElement('div');
    avatar.className = 'avatar';
    avatar.textContent = 'AI';

    const bubble = document.createElement('div');
    bubble.className = 'typing-bubble';
    bubble.innerHTML = `<span class="typing-dot"></span>
    <span class="typing-dot"></span><span class="typing-dot"></span>`;

    row.appendChild(avatar);
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    scrollToBottom();
}

function hideTyping() {
    const el = document.getElementById('typingIndicator');
    if (el) el.remove();
}

function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
}

// ── Main pipeline: text → SSE stream ─────────────────────────────
async function handleSendText() {
    const text = textInput.value.trim();
    if (!text) return;
    textInput.value = '';
    textInput.style.height = 'auto';
    await runStreamPipeline(text);
}

async function runStreamPipeline(userText) {
    // Stop any current audio immediately
    stopAllAudio();

    appendMessage('user', userText);
    conversationHistory.push({ role: 'user', content: userText });

    showTyping();
    setStatus('thinking', 'Thinking…');
    sendBtn.disabled = true;
    micBtn.disabled = true;

    let aiBubble = null;
    let fullText = '';
    let firstChunk = true;

    try {
        const response = await fetch('/api/chat/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                prompt: userText,
                history: conversationHistory.slice(0, -1),
                voice_id: selectedVoiceId,
                model_id: 'sonic-2',
                language: 'en',
                speed: selectedSpeed,
            }),
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(err.detail || 'Stream request failed');
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let sseBuffer = '';

        while (true) {
            const { value, done } = await reader.read();
            if (done) break;

            sseBuffer += decoder.decode(value, { stream: true });

            // Process all complete SSE lines in the buffer
            const lines = sseBuffer.split('\n');
            sseBuffer = lines.pop(); // keep last (possibly incomplete) line

            for (const line of lines) {
                if (!line.startsWith('data:')) continue;
                const raw = line.slice(5).trim();
                if (!raw) continue;

                let event;
                try { event = JSON.parse(raw); } catch { continue; }

                if (event.type === 'text') {
                    if (firstChunk) {
                        hideTyping();
                        aiBubble = createStreamingBubble();
                        firstChunk = false;
                        setStatus('speaking', 'Speaking…');
                    }
                    aiBubble.textContent += event.delta;
                    fullText += event.delta;
                    scrollToBottom();
                } else if (event.type === 'audio') {
                    // Audio chunk ready — enqueue for seamless playback
                    enqueueAudio(event.data);
                } else if (event.type === 'done') {
                    if (aiBubble) aiBubble.classList.remove('streaming');
                    conversationHistory.push({ role: 'assistant', content: fullText });
                } else if (event.type === 'error') {
                    hideTyping();
                    appendMessage('ai', `⚠️ Error: ${event.detail}`);
                    setStatus('idle', 'Error — try again');
                }
            }
        }

    } catch (err) {
        hideTyping();
        appendMessage('ai', `⚠️ Error: ${err.message}`);
        setStatus('idle', 'Error — try again');
        console.error('Pipeline error:', err);
    } finally {
        sendBtn.disabled = false;
        micBtn.disabled = false;
    }
}

// ── Microphone ────────────────────────────────────────────────────
micBtn.addEventListener('click', () => {
    if (isRecording) stopRecording();
    else startRecording();
});

async function startRecording() {
    try {
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
        const mimeType = ['audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus', 'audio/mp4']
            .find(m => MediaRecorder.isTypeSupported(m)) || '';

        mediaRecorder = new MediaRecorder(stream, mimeType ? { mimeType } : {});
        audioChunks = [];

        mediaRecorder.ondataavailable = e => { if (e.data ? e.size > 0) audioChunks.push(e.data); };
        mediaRecorder.onstop = async() => {
            stream.getTracks().forEach(t => t.stop());
            const blob = new Blob(audioChunks, { type: mediaRecorder.mimeType || 'audio/webm' });
            audioChunks = [];
            await transcribeAndSend(blob, mediaRecorder.mimeType || 'audio/webm');
        };

        mediaRecorder.start(250);
        isRecording = true;

        micBtn.classList.add('active');
        micIcon.style.display = 'none';
        stopIcon.style.display = 'block';
        recordingLabel.style.display = 'block';
        setStatus('recording', 'Recording…');

    } catch (err) {
        console.error('Mic access error:', err);
        setStatus('idle', 'Mic access denied');
        alert('Microphone access is required. Please allow it and try again.');
    }
}

function stopRecording() {
    if (mediaRecorder && mediaRecorder.state !== 'inactive') mediaRecorder.stop();
    isRecording = false;
    micBtn.classList.remove('active');
    micIcon.style.display = 'block';
    stopIcon.style.display = 'none';
    recordingLabel.style.display = 'none';
    setStatus('thinking', 'Transcribing…');
}

async function transcribeAndSend(audioBlob, mimeType) {
    try {
        const ext = mimeType.includes('ogg') ? 'ogg' : mimeType.includes('mp4') ? 'mp4' : 'webm';
        const formData = new FormData();
        formData.append('audio', audioBlob, `recording.${ext}`);

        const res = await fetch('/api/stt', { method: 'POST', body: formData });
        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: res.statusText }));
            throw new Error(err.detail || 'Transcription failed');
        }

        const { transcript } = await res.json();
        if (!transcript) { setStatus('idle', 'No speech detected'); return; }

        // Show transcript briefly in the input
        textInput.value = transcript;
        textInput.style.height = 'auto';
        textInput.style.height = Math.min(textInput.scrollHeight, 140) + 'px';

        await new Promise(r => setTimeout(r, 350));
        textInput.value = '';
        textInput.style.height = 'auto';

        await runStreamPipeline(transcript);

    } catch (err) {
        console.error('STT error:', err);
        appendMessage('ai', `⚠️ Transcription error: ${err.message}`);
        setStatus('idle', 'STT error — try again');
    }
}

// ── Clear ─────────────────────────────────────────────────────────
function clearChat() {
    conversationHistory = [];
    stopAllAudio();
    messagesEl.innerHTML = `
    <div class="welcome-hint">
      <span class="mic-icon-hint">🎙</span>
      <p>Press the microphone button and speak, or type a message below.</p>
    </div>`;
    setStatus('idle', 'Ready');
}

// ── Init ──────────────────────────────────────────────────────────
(async() => {
    setStatus('idle', 'Loading…');
    await loadVoices();
    setStatus('idle', 'Ready');
})();