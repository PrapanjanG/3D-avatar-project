import os
import io
import json
import re
import asyncio
from typing import List, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from dotenv import load_dotenv
import cartesia

load_dotenv()

app = FastAPI(title="Cartesia STT→TTS Demo")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ── Cartesia client ──────────────────────────────────────────────────────────
CARTESIA_API_KEY = os.getenv("CARTESIA_API_KEY")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
GEMINI_API_BASE  = os.getenv(
    "GEMINI_API_BASE",
    "https://generativelanguage.googleapis.com/v1beta/openai"
)

if not CARTESIA_API_KEY:
    print("⚠️  Warning: CARTESIA_API_KEY not set in .env")
else:
    print(f"✅ Cartesia API key loaded: {CARTESIA_API_KEY[:12]}...")

if not GEMINI_API_KEY:
    print("⚠️  Warning: GEMINI_API_KEY not set in .env")
else:
    print(f"✅ Gemini API key loaded: {GEMINI_API_KEY[:12]}...")

cartesia_client = cartesia.Cartesia(api_key=CARTESIA_API_KEY or "")

# ── Sentence boundary pattern ─────────────────────────────────────────────────
# Flush after . ! ? or after a comma/colon chunk that is long enough
SENTENCE_END = re.compile(r'(?<=[.!?])\s+|(?<=[.!?])$')
FLUSH_PUNCT  = re.compile(r'[,;:]\s*$')
MIN_CHUNK_CHARS = 60   # don't TTS very short fragments


# ── Models ───────────────────────────────────────────────────────────────────
class TTSRequest(BaseModel):
    text: str
    voice_id: str  = "a0e99841-438c-4a64-b679-ae501e7d6091"
    model_id: str  = "sonic-2"
    language: str  = "en"
    speed: str     = "normal"


class ChatRequest(BaseModel):
    prompt:  str
    history: List[dict] = []
    voice_id: str = "a0e99841-438c-4a64-b679-ae501e7d6091"
    model_id: str = "sonic-2"
    language: str = "en"
    speed: str    = "normal"


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "cartesia_key_set": bool(CARTESIA_API_KEY),
        "gemini_key_set":   bool(GEMINI_API_KEY),
    }


# ── STT ───────────────────────────────────────────────────────────────────────
@app.post("/api/stt")
async def speech_to_text(audio: UploadFile = File(...)):
    """Transcribe uploaded audio using Cartesia STT (ink-whisper)."""
    if not CARTESIA_API_KEY:
        raise HTTPException(status_code=500, detail="Cartesia API key not configured.")
    try:
        audio_bytes = await audio.read()
        if not audio_bytes:
            raise HTTPException(status_code=400, detail="Audio file is empty.")

        audio_file = (
            audio.filename or "recording.webm",
            io.BytesIO(audio_bytes),
            audio.content_type or "audio/webm",
        )
        result     = cartesia_client.stt.transcribe(
            file=audio_file, model="ink-whisper", language="en"
        )
        transcript = result.text.strip()
        print(f"✅ STT transcript: {transcript!r}")

        if not transcript:
            raise HTTPException(
                status_code=422, detail="Could not transcribe audio — no speech detected."
            )
        return JSONResponse(content={"transcript": transcript})

    except HTTPException:
        raise
    except cartesia.AuthenticationError:
        raise HTTPException(status_code=401, detail="Invalid Cartesia API key.")
    except Exception as e:
        print(f"❌ STT error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"STT error: {type(e).__name__}: {str(e)}")


# ── TTS helper (sync, runs in thread) ────────────────────────────────────────
def _tts_bytes_sync(text: str, voice_id: str, model_id: str,
                    language: str, speed: str) -> bytes:
    """Call Cartesia TTS synchronously and return raw WAV bytes."""
    buf = io.BytesIO()
    for chunk in cartesia_client.tts.bytes(
        model_id=model_id,
        transcript=text,
        voice={"mode": "id", "id": voice_id},
        output_format={"container": "wav", "encoding": "pcm_f32le", "sample_rate": 44100},
        language=language,
        speed=speed,
    ):
        buf.write(chunk)
    return buf.getvalue()


# ── Streaming chat + TTS endpoint ─────────────────────────────────────────────
@app.post("/api/chat/stream")
async def chat_stream(payload: ChatRequest):
    """
    SSE stream.  Each event is a JSON object with one of:
      {"type": "text",  "delta": "<word(s)>"}       — append to UI bubble
      {"type": "audio", "data": "<base64 wav>"}      — play this WAV chunk
      {"type": "done"}                               — conversation turn complete
      {"type": "error", "detail": "..."}
    """
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="Gemini API key not configured.")
    if not payload.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty.")

    messages = []
    for item in payload.history:
        if not isinstance(item, dict):
            continue
        role    = item.get("role")
        content = item.get("content")
        if role in {"user", "assistant"} and isinstance(content, str):
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": payload.prompt.strip()})

    voice_id = payload.voice_id
    model_id = payload.model_id
    language = payload.language
    speed    = payload.speed

    async def event_generator() -> AsyncGenerator[str, None]:
        import base64

        def sse(obj: dict) -> str:
            return f"data: {json.dumps(obj)}\n\n"

        # ── 1. Stream LLM tokens ──────────────────────────────────────────
        text_buffer   = ""   # accumulates text waiting for a flush boundary
        full_response = ""   # entire response text for history

        # TTS jobs: queue of sentences ready to synthesise
        tts_queue: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        # ── 2. TTS worker — runs concurrently, drains the queue ───────────
        async def tts_worker():
            while True:
                sentence = await tts_queue.get()
                if sentence is None:          # sentinel → done
                    tts_queue.task_done()
                    break
                try:
                    wav_bytes = await loop.run_in_executor(
                        None, _tts_bytes_sync,
                        sentence, voice_id, model_id, language, speed
                    )
                    b64 = base64.b64encode(wav_bytes).decode()
                    audio_events.append(sse({"type": "audio", "data": b64}))
                except Exception as e:
                    print(f"⚠️ TTS worker error: {e}")
                finally:
                    tts_queue.task_done()

        # We collect audio SSE events here so the generator can yield them
        # interleaved with text events in the main loop below.
        audio_events: list[str] = []

        worker_task = asyncio.create_task(tts_worker())

        def flush_sentence(text: str):
            """Send text to TTS queue if it's worth synthesising."""
            text = text.strip()
            if text:
                tts_queue.put_nowait(text)

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                async with client.stream(
                    "POST",
                    f"{GEMINI_API_BASE.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {GEMINI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "gemini-2.5-flash",
                        "messages": messages,
                        "stream": True,
                    },
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        yield sse({"type": "error", "detail": body.decode()[:400]})
                        return

                    async for raw_line in resp.aiter_lines():
                        # Drain any ready audio events first
                        while audio_events:
                            yield audio_events.pop(0)

                        if not raw_line.startswith("data:"):
                            continue
                        payload_str = raw_line[5:].strip()
                        if payload_str == "[DONE]":
                            break

                        try:
                            chunk = json.loads(payload_str)
                        except json.JSONDecodeError:
                            continue

                        delta = (
                            chunk.get("choices", [{}])[0]
                            .get("delta", {})
                            .get("content", "")
                        )
                        if not delta:
                            continue

                        # Stream text token to UI immediately
                        yield sse({"type": "text", "delta": delta})
                        text_buffer   += delta
                        full_response += delta

                        # Check for sentence boundary → flush to TTS
                        parts = SENTENCE_END.split(text_buffer)
                        if len(parts) > 1:
                            # Everything except the last incomplete fragment
                            for sentence in parts[:-1]:
                                flush_sentence(sentence)
                            text_buffer = parts[-1]
                        elif len(text_buffer) >= MIN_CHUNK_CHARS and FLUSH_PUNCT.search(text_buffer):
                            flush_sentence(text_buffer)
                            text_buffer = ""

            # Flush whatever remains in the text buffer
            if text_buffer.strip():
                flush_sentence(text_buffer)

            # Signal TTS worker to stop
            tts_queue.put_nowait(None)
            await worker_task

            # Yield any remaining audio events
            while audio_events:
                yield audio_events.pop(0)

            yield sse({"type": "done", "full_text": full_response})

        except Exception as e:
            print(f"❌ Stream error: {type(e).__name__}: {e}")
            # Make sure worker exits cleanly
            tts_queue.put_nowait(None)
            yield sse({"type": "error", "detail": str(e)})

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering if behind proxy
        },
    )


# ── Non-streaming fallback TTS (kept for voice-test button) ──────────────────
@app.post("/api/tts")
async def text_to_speech(payload: TTSRequest):
    if not payload.text.strip():
        raise HTTPException(status_code=400, detail="Text cannot be empty.")
    try:
        loop      = asyncio.get_event_loop()
        wav_bytes = await loop.run_in_executor(
            None, _tts_bytes_sync,
            payload.text, payload.voice_id, payload.model_id,
            payload.language, payload.speed,
        )
        return StreamingResponse(
            io.BytesIO(wav_bytes),
            media_type="audio/wav",
            headers={"Content-Disposition": "inline; filename=speech.wav"},
        )
    except cartesia.AuthenticationError:
        raise HTTPException(status_code=401, detail="Invalid Cartesia API key.")
    except cartesia.BadRequestError as e:
        raise HTTPException(status_code=400, detail=f"Cartesia bad request: {str(e)}")
    except Exception as e:
        print(f"❌ TTS error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"TTS error: {type(e).__name__}: {str(e)}")


# ── Voices ────────────────────────────────────────────────────────────────────
@app.get("/api/voices")
async def get_voices():
    try:
        voices_page = cartesia_client.voices.list()
        voices = []
        for v in voices_page:
            try:
                gender_val = getattr(v, "gender", None)
                voices.append({
                    "id":          str(v.id),
                    "name":        str(v.name),
                    "description": str(getattr(v, "description", "") or ""),
                    "language":    str(getattr(v, "language", "en") or "en"),
                    "gender":      str(gender_val) if gender_val else "",
                })
            except Exception as ve:
                print(f"Skipping voice: {ve}")
        print(f"✅ Loaded {len(voices)} voices")
        return JSONResponse(content={"voices": voices})
    except cartesia.AuthenticationError:
        raise HTTPException(status_code=401, detail="Invalid Cartesia API key.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch voices: {str(e)}")