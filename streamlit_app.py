from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import requests
import os
from typing import List
import json
import html

def escapeHtmlPython(text):
    """Escape HTML characters in Python."""
    return html.escape(str(text))

# Configuration
ES_HOSTS = os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200").split(",")
VECTOR_INDEX = "anomalies_vectorized"
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
if not OLLAMA_HOST.startswith("http"):
    OLLAMA_HOST = "http://" + OLLAMA_HOST
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")
OLLAMA_URL = OLLAMA_HOST.rstrip("/") + "/api/generate"

# Initialize
app = FastAPI()
es = Elasticsearch(ES_HOSTS)
embedder = SentenceTransformer('all-MiniLM-L6-v2')

def get_anomaly_stats():
    """Get statistics about anomalies in the database."""
    try:
        resp = es.count(index=VECTOR_INDEX, body={"query": {"match_all": {}}})
        total = resp.get('count', 0)
        
        # Get recent anomalies
        resp = es.search(
            index=VECTOR_INDEX,
            body={
                "size": 5,
                "sort": [{"@timestamp": {"order": "desc"}}],
                "query": {"match_all": {}}
            }
        )
        recent = [hit["_source"] for hit in resp["hits"]["hits"]]
        return {"total": total, "recent": recent}
    except Exception as e:
        return {"total": 0, "recent": [], "error": str(e)}

def retrieve_logs_semantic(question: str, top_k: int = 5) -> List[str]:
    """Retrieve similar anomalies from Elasticsearch using semantic search."""
    try:
        query_vec = embedder.encode([question])[0]
        resp = es.search(
            index=VECTOR_INDEX,
            body={
                "size": top_k,
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": {"query_vector": query_vec.tolist()}
                        }
                    }
                }
            }
        )
        return [hit["_source"].get("raw_log") or hit["_source"].get("message", "") for hit in resp["hits"]["hits"]]
    except Exception as e:
        return [f"Error retrieving logs: {str(e)}"]

def generate_answer_with_ollama(question: str, context: List[str]) -> str:
    """Generate answer using Ollama LLM with RAG."""
    if not context:
        return "No anomalies found in the database."
    
    context_str = "\n".join([c for c in context if c])
    prompt = (
        "You are a log analysis expert. Based on the provided anomaly logs, "
        "answer the user's question concisely and helpfully.\n\n"
        f"Question: {question}\n\n"
        f"Anomaly Logs Context:\n{context_str}\n\n"
        "Answer:"
    )
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False
            },
            timeout=120
        )
        response.raise_for_status()
        result = response.json()
        return result.get("response", "No response from LLM").strip()
    except Exception as e:
        return f"Error generating answer: {str(e)}"

@app.get("/", response_class=HTMLResponse)
async def get_home():
    stats = get_anomaly_stats()
    total_anomalies = stats.get("total", 0)
    recent = stats.get("recent", [])
    
    recent_html = ""
    for log in recent:
        raw_log = log.get("raw_log", "")[:100]
        recent_html += f'<div class="recent-item"><code>{escapeHtmlPython(raw_log)}...</code></div>'
    
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Anomaly Chat</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, sans-serif; background: #f5f5f5; }}
            .navbar {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 16px 24px; display: flex; justify-content: space-between; align-items: center; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
            .navbar h2 {{ font-size: 24px; }}
            .navbar a {{ color: white; text-decoration: none; margin-left: 20px; cursor: pointer; }}
            .navbar a:hover {{ opacity: 0.8; }}
            .container {{ display: flex; height: calc(100vh - 60px); }}
            .sidebar {{ width: 280px; background: white; padding: 24px; border-right: 1px solid #e0e0e0; overflow-y: auto; }}
            .sidebar h3 {{ color: #667eea; margin-bottom: 16px; font-size: 16px; }}
            .stat-card {{ background: #f9f9f9; padding: 16px; border-radius: 8px; margin-bottom: 12px; border-left: 4px solid #667eea; }}
            .stat-value {{ font-size: 32px; font-weight: bold; color: #667eea; }}
            .stat-label {{ font-size: 12px; color: #999; margin-top: 4px; }}
            .recent-item {{ padding: 8px; margin: 6px 0; background: #f0f0f0; border-radius: 4px; font-size: 11px; font-family: monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
            .main-content {{ flex: 1; display: flex; flex-direction: column; }}
            .chat-container {{ flex: 1; display: flex; flex-direction: column; margin: 16px; background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); overflow: hidden; }}
            .chat-header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; text-align: center; }}
            .chat-header h1 {{ font-size: 24px; margin-bottom: 4px; }}
            .chat-messages {{ flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }}
            .message {{ display: flex; gap: 12px; animation: slideIn 0.3s ease-out; }}
            @keyframes slideIn {{ from {{ opacity: 0; transform: translateY(10px); }} to {{ opacity: 1; transform: translateY(0); }} }}
            .message.user {{ justify-content: flex-end; }}
            .message.assistant {{ justify-content: flex-start; }}
            .message-bubble {{ max-width: 65%; padding: 12px 16px; border-radius: 12px; word-wrap: break-word; font-size: 14px; line-height: 1.5; }}
            .message.user .message-bubble {{ background: #667eea; color: white; }}
            .message.assistant .message-bubble {{ background: #f0f0f0; color: #333; }}
            .logs-section {{ background: #f9f9f9; padding: 10px; border-left: 4px solid #764ba2; margin: 8px 0; }}
            .logs-title {{ font-weight: 600; font-size: 12px; color: #764ba2; margin-bottom: 8px; }}
            .log {{ background: #fff; padding: 6px; border-radius: 3px; margin: 4px 0; font-family: 'Courier New', monospace; font-size: 11px; color: #666; max-height: 60px; overflow-y: auto; }}
            .chat-input-area {{ padding: 16px; border-top: 1px solid #e0e0e0; background: #fafafa; }}
            .input-wrapper {{ display: flex; gap: 10px; }}
            input[type="text"] {{ flex: 1; padding: 10px 14px; border: 1px solid #ddd; border-radius: 20px; font-size: 14px; }}
            input[type="text"]:focus {{ outline: none; border-color: #667eea; }}
            button {{ padding: 10px 24px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border: none; border-radius: 20px; cursor: pointer; font-size: 14px; font-weight: 600; }}
            button:hover {{ opacity: 0.9; }}
            .empty-state {{ display: flex; flex-direction: column; justify-content: center; align-items: center; height: 100%; color: #999; }}
            .empty-state-icon {{ font-size: 48px; margin-bottom: 12px; }}
            .loading {{ text-align: center; color: #667eea; font-size: 13px; }}
            .page {{ display: none; }}
            .page.active {{ display: flex; flex-direction: column; flex: 1; }}
            .dashboard-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; padding: 20px; }}
            .dashboard-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
            .dashboard-card h3 {{ color: #667eea; margin-bottom: 12px; }}
            .dashboard-card .value {{ font-size: 28px; font-weight: bold; color: #333; }}
        </style>
    </head>
    <body>
        <div class="navbar">
            <h2>🔍 Anomaly Chat</h2>
            <div>
                <a onclick="showPage('chat')">💬 Chat</a>
                <a onclick="showPage('dashboard')">📊 Dashboard</a>
            </div>
        </div>

        <div class="container">
            <div class="sidebar">
                <h3>📈 Statistics</h3>
                <div class="stat-card">
                    <div class="stat-value">{total_anomalies}</div>
                    <div class="stat-label">Total Anomalies</div>
                </div>
                
                <h3 style="margin-top: 20px;">🔔 Recent</h3>
                {recent_html if recent_html else '<div style="color: #999; font-size: 12px;">No recent anomalies</div>'}
            </div>

            <div class="main-content">
                <!-- Chat Page -->
                <div id="chat" class="page active">
                    <div class="chat-container">
                        <div class="chat-header">
                            <h1>💬 Chat with Anomaly Assistant</h1>
                            <p>Ask questions about your anomalies - powered by RAG</p>
                        </div>
                        
                        <div class="chat-messages" id="chatMessages">
                            <div class="empty-state">
                                <div class="empty-state-icon">💬</div>
                                <p>Start asking questions...</p>
                            </div>
                        </div>
                        
                        <div class="chat-input-area">
                            <div class="input-wrapper">
                                <input type="text" id="questionInput" placeholder="Ask about anomalies..." autocomplete="off">
                                <button onclick="sendMessage()">Send</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Dashboard Page -->
                <div id="dashboard" class="page">
                    <div style="background: white; flex: 1; overflow-y: auto;">
                        <div class="dashboard-grid">
                            <div class="dashboard-card">
                                <h3>📊 Total Anomalies</h3>
                                <div class="value">{total_anomalies}</div>
                            </div>
                            <div class="dashboard-card">
                                <h3>⏰ Recent Anomalies</h3>
                                <div class="value">{len(recent)}</div>
                                <p style="color: #999; font-size: 12px; margin-top: 8px;">Last 5 entries shown</p>
                            </div>
                            <div class="dashboard-card">
                                <h3>🤖 RAG Status</h3>
                                <div class="value" style="color: #28a745;">Active</div>
                            </div>
                        </div>
                        
                        <div style="padding: 20px;">
                            <h3 style="color: #667eea; margin-bottom: 12px;">🔔 Recent Anomalies</h3>
                            <div style="background: #f9f9f9; border-radius: 8px; padding: 12px;">
                                {recent_html if recent_html else '<p style="color: #999;">No recent anomalies</p>'}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            const chatMessages = document.getElementById('chatMessages');
            const questionInput = document.getElementById('questionInput');

            function scrollToBottom() {{
                chatMessages.scrollTop = chatMessages.scrollHeight;
            }}

            function showPage(pageName) {{
                document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
                document.getElementById(pageName).classList.add('active');
            }}

            function escapeHtml(text) {{
                const map = {{'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'}};
                return text.replace(/[&<>"']/g, m => map[m]);
            }}

            function addMessage(role, message, logs = null) {{
                const messageDiv = document.createElement('div');
                messageDiv.className = `message ${{role}}`;
                
                let html = `<div class="message-bubble">${{escapeHtml(message)}}</div>`;
                
                if (logs && logs.length > 0) {{
                    html += `<div class="logs-section">
                        <div class="logs-title">📊 Retrieved Anomalies:</div>
                        ${{logs.map(log => `<div class="log">${{escapeHtml(log)}}</div>`).join('')}}
                    </div>`;
                }}
                
                messageDiv.innerHTML = html;
                chatMessages.appendChild(messageDiv);
                scrollToBottom();
            }}

            async function sendMessage() {{
                const question = questionInput.value.trim();
                if (!question) return;

                questionInput.value = '';
                
                const emptyState = chatMessages.querySelector('.empty-state');
                if (emptyState) emptyState.remove();

                addMessage('user', question);

                const loadingDiv = document.createElement('div');
                loadingDiv.className = 'loading';
                loadingDiv.innerHTML = '<p>🔄 Analyzing...</p>';
                chatMessages.appendChild(loadingDiv);
                scrollToBottom();

                try {{
                    const response = await fetch('/query', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ question: question }})
                    }});

                    loadingDiv.remove();

                    const data = await response.json();
                    if (data.error) {{
                        addMessage('assistant', `Error: ${{data.error}}`);
                    }} else {{
                        addMessage('assistant', data.answer, data.logs);
                    }}
                }} catch (error) {{
                    loadingDiv.remove();
                    addMessage('assistant', `Error: ${{error.message}}`);
                }}
            }}

            questionInput.addEventListener('keypress', (e) => {{
                if (e.key === 'Enter') sendMessage();
            }});
        </script>
    </body>
    </html>
    """

@app.post("/query")
async def query(request: Request):
    data = await request.json()
    question = data.get("question", "")
    
    if not question:
        return {"error": "No question provided"}
    
    try:
        logs = retrieve_logs_semantic(question)
        answer = generate_answer_with_ollama(question, logs)
        return {"answer": answer, "logs": logs}
    except Exception as e:
        return {"error": str(e), "logs": []}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

